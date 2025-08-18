#include <Storages/MergeTree/MergeTreeIndexSurfFilterText.h>

#include <Storages/MergeTree/MergeTreeIndexSurfFilterText.h>

// SuRF-based Text Filter Index Implementation
//
// This implementation uses SuRF (Succinct Range Filter) for text index filtering.
// The filtering logic is designed to eliminate granules that definitely cannot contain matches:
//
// 1. Empty query filter -> no search tokens -> no possible matches
// 2. Empty granule filter -> no indexed tokens -> no possible matches
// 3. Both non-empty -> potential match exists, forward to query processing
//
// This eliminates unnecessary work while ensuring no valid matches are missed.

#include <Columns/ColumnArray.h>

#include <Columns/ColumnArray.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleSurfFilterText::MergeTreeIndexGranuleSurfFilterText(
    const String & index_name_, size_t columns_number, const SurfFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , surf_filters()
    , has_elems(false)
{
    // Initialize vector with emplace_back to avoid copy/move issues
    surf_filters.reserve(columns_number);
    for (std::size_t i = 0; i < columns_number; ++i)
    {
        surf_filters.emplace_back(params);
    }
}

void MergeTreeIndexGranuleSurfFilterText::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fulltext index {}.", backQuote(index_name));

    for (const auto & surf_filter : surf_filters)
        surf_filter.serialize(ostr);
}

void MergeTreeIndexGranuleSurfFilterText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    for (auto & surf_filter : surf_filters)
    {
        surf_filter.deserialize(istr);
    }
    has_elems = true;
}


size_t MergeTreeIndexGranuleSurfFilterText::memoryUsageBytes() const
{
    size_t sum = 0;
    for (const auto & surf_filter : surf_filters)
        sum += surf_filter.memoryUsageBytes();
    return sum;
}


MergeTreeIndexAggregatorSurfFilterText::MergeTreeIndexAggregatorSurfFilterText(
    const Names & index_columns_, const String & index_name_, const SurfFilterParameters & params_, TokenExtractorPtr token_extractor_)
    : index_columns(index_columns_)
    , index_name(index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , granule(std::make_shared<MergeTreeIndexGranuleSurfFilterText>(index_name, index_columns.size(), params))
    , collected_tokens(index_columns.size())
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSurfFilterText::getGranuleAndReset()
{
    // Sort and add all collected tokens to SuRF filters before finalizing
    for (size_t col = 0; col < collected_tokens.size(); ++col)
    {
        auto & tokens = collected_tokens[col];

        // Sort tokens for consistent insertion order and better SuRF performance
        std::sort(tokens.begin(), tokens.end());

        // Build SuRF filter from all sorted tokens at once
        granule->surf_filters[col] = SurfFilter(tokens, params);
    }

    auto new_granule = std::make_shared<MergeTreeIndexGranuleSurfFilterText>(index_name, index_columns.size(), params);
    new_granule.swap(granule);

    collected_tokens.clear();
    return new_granule;
}
void MergeTreeIndexAggregatorSurfFilterText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;

        if (isArray(column_with_type.type))
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & column_offsets = column_array.getOffsets();
            const auto & column_key = column_array.getData();

            for (size_t i = 0; i < rows_read; ++i)
            {
                size_t element_start_row = column_offsets[current_position - 1];
                size_t elements_size = column_offsets[current_position] - element_start_row;

                for (size_t row_num = 0; row_num < elements_size; ++row_num)
                {
                    auto ref = column_key.getDataAt(element_start_row + row_num);
                    token_extractor->stringPaddedToTokens(ref.data, ref.size, collected_tokens[col]);
                }

                current_position += 1;
            }
        }
        else
        {
            for (size_t i = 0; i < rows_read; ++i)
            {
                auto ref = column->getDataAt(current_position + i);
                token_extractor->stringPaddedToTokens(ref.data, ref.size, collected_tokens[col]);
            }
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionSurfFilterText::MergeTreeConditionSurfFilterText(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const Block & index_sample_block,
    const SurfFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    : index_columns(index_sample_block.getNames())
    , index_data_types(index_sample_block.getNamesAndTypesList().getTypes())
    , params(params_)
    , token_extractor(token_extactor_)
{
    if (!predicate)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate, context, [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

/// Keep in-sync with MergeTreeConditionGinFilter::alwaysUnknownOrTrue
bool MergeTreeConditionSurfFilterText::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_EQUALS,
         RPNElement::FUNCTION_NOT_EQUALS,
         RPNElement::FUNCTION_HAS,
         RPNElement::FUNCTION_IN,
         RPNElement::FUNCTION_NOT_IN,
         RPNElement::FUNCTION_MULTI_SEARCH,
         RPNElement::FUNCTION_MATCH,
         RPNElement::FUNCTION_HAS_ANY,
         RPNElement::FUNCTION_HAS_ALL,
         RPNElement::ALWAYS_FALSE});
}

/// Keep in-sync with MergeTreeIndexConditionGin::mayBeTrueOnTranuleInPart
bool MergeTreeConditionSurfFilterText::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleSurfFilterText> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleSurfFilterText>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SurfFilter index condition got a granule with the wrong type.");

    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (
            element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_HAS)
        {
            // Extract tokens from the query string using the same tokenizer
            // For EQUALS/NOT_EQUALS/HAS functions, there should be exactly one key
            if (element.keys.empty())
            {
                rpn_stack.emplace_back(true, true); // Cannot filter on empty key set
                continue;
            }

            const auto & query_string = element.keys[0]; // Single-value functions use only first key
            std::vector<std::string> query_tokens = token_extractor->getTokens(query_string.data(), query_string.size());

            rpn_stack.emplace_back(granule->surf_filters[element.key_column].contains(query_tokens), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_surf_filters.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const size_t key_idx = element.set_key_position[column];
                if (key_idx >= granule->surf_filters.size())
                    continue;

                const auto & surf_filters = element.set_surf_filters[column];
                for (size_t row = 0; row < surf_filters.size(); ++row)
                {
                    // Extract tokens from the query string for this row
                    std::vector<std::string> query_tokens;
                    if (row < element.keys.size())
                    {
                        const auto & query_string = element.keys[row];
                        query_tokens = token_extractor->getTokens(query_string.data(), query_string.size());
                    }
                    result[row] = result[row] && granule->surf_filters[key_idx].contains(query_tokens);
                }
            }

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == RPNElement::FUNCTION_MULTI_SEARCH || element.function == RPNElement::FUNCTION_HAS_ANY
            || element.function == RPNElement::FUNCTION_HAS_ALL)
        {
            std::vector<bool> result;

            // Process each search string stored in keys
            for (const auto & search_string : element.keys)
            {
                // Extract tokens from the search string
                std::vector<std::string> query_tokens;
                if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
                {
                    // For multiSearchAny, use substring tokenization (partial matches)
                    query_tokens = token_extractor->getTokens(search_string.data(), search_string.size());
                }
                else
                {
                    // For hasAny/hasAll, use full string tokenization
                    query_tokens = token_extractor->getTokens(search_string.data(), search_string.size());
                }

                // Check if the granule contains all tokens from this search string
                bool match = granule->surf_filters[element.key_column].contains(query_tokens);
                result.push_back(match);
            }

            if (element.function == RPNElement::FUNCTION_HAS_ALL)
                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), false) == std::end(result), true);
            else
                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
        }
        else if (element.function == RPNElement::FUNCTION_MATCH)
        {
            // For FUNCTION_MATCH, use the stored keys and extract tokens during query processing
            if (!element.keys.empty())
            {
                bool any_match = false;

                // Check each alternative/required substring
                for (const auto & search_string : element.keys)
                {
                    // Extract tokens from the search string
                    std::vector<std::string> query_tokens = token_extractor->getTokens(search_string.data(), search_string.size());

                    // Check if the granule contains all tokens from this search string
                    if (granule->surf_filters[element.key_column].contains(query_tokens))
                    {
                        any_match = true;
                        break; // Found a matching alternative
                    }
                }

                rpn_stack.emplace_back(any_match, true);
            }
            else
            {
                // No search strings - should not happen, but be safe
                rpn_stack.emplace_back(true, true);
            }
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in SurfFilterCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in SurfFilterCondition::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

std::optional<size_t> MergeTreeConditionSurfFilterText::getKeyIndex(const std::string & key_column_name)
{
    const auto it = std::ranges::find(index_columns, key_column_name);
    return it == index_columns.end() ? std::nullopt : std::make_optional<size_t>(std::ranges::distance(index_columns.cbegin(), it));
}

bool MergeTreeConditionSurfFilterText::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            /// Check constant like in KeyCondition

            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();
        auto function_name = function_node.getFunctionName();

        size_t arguments_size = function_node.getArgumentsSize();
        if (arguments_size != 2)
            return false;

        auto left_argument = function_node.getArgumentAt(0);
        auto right_argument = function_node.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (tryPrepareSetSurfFilter(left_argument, right_argument, out))
            {
                if (function_name == "notIn")
                {
                    out.function = RPNElement::FUNCTION_NOT_IN;
                    return true;
                }
                if (function_name == "in")
                {
                    out.function = RPNElement::FUNCTION_IN;
                    return true;
                }
            }
        }
        else if (
            function_name == "equals" || function_name == "notEquals" || function_name == "has" || function_name == "mapContains"
            || function_name == "mapContainsKey" || function_name == "mapContainsKeyLike" || function_name == "mapContainsValue"
            || function_name == "mapContainsValueLike" || function_name == "match" || function_name == "like" || function_name == "notLike"
            || function_name.starts_with("hasToken") || function_name == "startsWith" || function_name == "endsWith"
            || function_name == "multiSearchAny" || function_name == "hasAny" || function_name == "hasAll")
        {
            Field const_value;
            DataTypePtr const_type;

            if (right_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, left_argument, const_type, const_value, out))
                    return true;
            }
            else if (left_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseTreeEquals(function_name, right_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionSurfFilterText::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;

    const auto column_name = key_node.getColumnName();
    auto key_index = getKeyIndex(column_name);
    const auto map_key_index = getKeyIndex(fmt::format("mapKeys({})", column_name));
    const auto map_value_index = getKeyIndex(fmt::format("mapValues({})", column_name));

    if (key_node.isFunction())
    {
        auto key_function_node = key_node.toFunctionNode();
        auto key_function_node_function_name = key_function_node.getFunctionName();

        if (key_function_node_function_name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exist in the map
              * we return default the value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exist.
              */
            if (value_field == value_type->getDefault())
                return false;

            auto first_argument = key_function_node.getArgumentAt(0);
            const auto map_column_name = first_argument.getColumnName();
            if (const auto map_keys_index = getKeyIndex(fmt::format("mapKeys({})", map_column_name)))
            {
                auto second_argument = key_function_node.getArgumentAt(1);
                DataTypePtr const_type;

                if (second_argument.tryGetConstant(const_value, const_type))
                {
                    key_index = map_keys_index;

                    auto const_data_type = WhichDataType(const_type);
                    if (!const_data_type.isStringOrFixedString() && !const_data_type.isArray())
                        return false;
                }
                else
                {
                    return false;
                }
            }
            else if (const auto map_values_exists = getKeyIndex(fmt::format("mapValues({})", map_column_name)))
            {
                key_index = map_values_exists;
            }
            else
            {
                return false;
            }
        }
    }

    const auto lowercase_key_index = getKeyIndex(fmt::format("lower({})", column_name));
    const auto is_has_token_case_insensitive = function_name.starts_with("hasTokenCaseInsensitive");
    if (const auto is_case_insensitive_scenario = is_has_token_case_insensitive && lowercase_key_index;
        function_name.starts_with("hasToken") && ((!is_has_token_case_insensitive && key_index) || is_case_insensitive_scenario))
    {
        out.key_column = is_case_insensitive_scenario ? *lowercase_key_index : *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;

        auto value = const_value.safeGet<String>();
        if (is_case_insensitive_scenario)
            std::ranges::transform(value, value.begin(), [](const auto & c) { return static_cast<char>(std::tolower(c)); });


        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }

    if (!key_index && !map_key_index && !map_value_index)
        return false;

    if (map_key_index)
    {
        if (function_name == "has" || function_name == "mapContainsKey" || function_name == "mapContains")
        {
            out.key_column = *key_index;
            out.function = RPNElement::FUNCTION_HAS;
            auto & value = const_value.safeGet<String>();

            // Extract keys from the search value for contains() method
            out.keys.clear();
            out.keys.push_back(value);

            return true;
        }
        if (function_name == "mapContainsKeyLike")
        {
            out.key_column = *key_index;
            out.function = RPNElement::FUNCTION_HAS;
            auto & value = const_value.safeGet<String>();

            // Extract keys from the search value for contains() method
            out.keys.clear();
            out.keys.push_back(value);

            return true;
        }
        // When map_key_index is set, we shouldn't use ngram/token bf for other functions
        return false;
    }

    if (map_value_index)
    {
        if (function_name == "mapContainsValue")
        {
            out.key_column = *map_value_index;
            out.function = RPNElement::FUNCTION_HAS;

            auto & value = const_value.safeGet<String>();

            // Extract keys from the search value for contains() method
            out.keys.clear();
            out.keys.push_back(value);

            return true;
        }
        if (function_name == "mapContainsValueLike")
        {
            out.key_column = *map_value_index;
            out.function = RPNElement::FUNCTION_HAS;
            auto & value = const_value.safeGet<String>();

            // Extract keys from the search value for contains() method
            out.keys.clear();
            out.keys.push_back(value);

            return true;
        }
        // When map_value_index is set, we shouldn't use ngram/token bf for other functions
        return false;
    }

    if (function_name == "has")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_HAS;
        auto & value = const_value.safeGet<String>();

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        const auto & value = const_value.safeGet<String>();

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "equals")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        const auto & value = const_value.safeGet<String>();

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "like")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.surf_filter = std::make_unique<SurfFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToSurfFilter(value.data(), value.size(), *out.surf_filter);

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "notLike")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.surf_filter = std::make_unique<SurfFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToSurfFilter(value.data(), value.size(), *out.surf_filter);

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "startsWith")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.surf_filter = std::make_unique<SurfFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToSurfFilter(value.data(), value.size(), *out.surf_filter, true, false);

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "endsWith")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.surf_filter = std::make_unique<SurfFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToSurfFilter(value.data(), value.size(), *out.surf_filter, false, true);

        // Extract keys from the search value for contains() method
        out.keys.clear();
        out.keys.push_back(value);

        return true;
    }
    if (function_name == "multiSearchAny" || function_name == "hasAny" || function_name == "hasAll")
    {
        out.key_column = *key_index;
        out.function = function_name == "multiSearchAny" ? RPNElement::FUNCTION_MULTI_SEARCH
            : function_name == "hasAny"                  ? RPNElement::FUNCTION_HAS_ANY
                                                         : RPNElement::FUNCTION_HAS_ALL;

        // Store the search strings as keys for token extraction during query processing
        out.keys.clear();
        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            const auto & value = element.safeGet<String>();
            out.keys.push_back(value);
        }
        return true;
    }
    if (function_name == "match")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_MATCH;
        // Don't create SuRF filter during condition building - we'll use getTokens() during query processing

        auto & value = const_value.safeGet<String>();
        String required_substring;
        bool dummy_is_trivial;
        bool dummy_required_substring_is_prefix;
        std::vector<String> alternatives;
        OptimizedRegularExpression::analyze(value, required_substring, dummy_is_trivial, dummy_required_substring_is_prefix, alternatives);

        if (required_substring.empty() && alternatives.empty())
            return false;

        /// Store the search strings as keys for token extraction during query processing
        if (!alternatives.empty())
        {
            // Store alternatives as keys
            out.keys.clear();
            for (const auto & alternative : alternatives)
            {
                out.keys.push_back(alternative);
            }
        }
        else
        {
            // Store required substring as key
            out.keys.clear();
            out.keys.push_back(required_substring);
        }

        return true;
    }

    return false;
}

bool MergeTreeConditionSurfFilterText::tryPrepareSetSurfFilter(
    const RPNBuilderTreeNode & left_argument, const RPNBuilderTreeNode & right_argument, RPNElement & out)
{
    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    auto left_argument_function_node_optional = left_argument.toFunctionNodeOrNull();

    if (left_argument_function_node_optional && left_argument_function_node_optional->getFunctionName() == "tuple")
    {
        const auto & left_argument_function_node = *left_argument_function_node_optional;
        size_t left_argument_function_node_arguments_size = left_argument_function_node.getArgumentsSize();

        for (size_t i = 0; i < left_argument_function_node_arguments_size; ++i)
        {
            if (const auto key = getKeyIndex(left_argument_function_node.getArgumentAt(i).getColumnName()))
            {
                key_tuple_mapping.emplace_back(i, *key);
                data_types.push_back(index_data_types[*key]);
            }
        }
    }
    else if (const auto key = getKeyIndex(left_argument.getColumnName()))
    {
        key_tuple_mapping.emplace_back(0, *key);
        data_types.push_back(index_data_types[*key]);
    }

    if (key_tuple_mapping.empty())
        return false;

    auto future_set = right_argument.tryGetPreparedSet(data_types);
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(right_argument.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & prepared_set_data_type : prepared_set->getDataTypes())
    {
        auto prepared_set_data_type_id = prepared_set_data_type->getTypeId();
        if (prepared_set_data_type_id != TypeIndex::String && prepared_set_data_type_id != TypeIndex::FixedString)
            return false;
    }

    std::vector<std::vector<SurfFilter>> surf_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    size_t prepared_set_total_row_count = prepared_set->getTotalRowCount();

    for (const auto & elem : key_tuple_mapping)
    {
        surf_filters.emplace_back();
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];

        for (size_t row = 0; row < prepared_set_total_row_count; ++row)
        {
            surf_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            token_extractor->stringPaddedToSurfFilter(ref.data, ref.size, surf_filters.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_surf_filters = std::move(surf_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexSurfFilterText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSurfFilterText>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSurfFilterText::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorSurfFilterText>(index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexSurfFilterText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionSurfFilterText>(predicate, context, index.sample_block, params, token_extractor.get());
}

/// Convert Bloom filter parameters to SuRF parameters for text indexes
///
/// Bloom filter parameters (from original implementation):
/// - filter_size_bytes: Size of the Bloom filter in bytes (e.g., 256, 512)
/// - num_hash_functions: Number of hash functions used (typically 2-8)
/// - random_seed: Seed for hash functions (not used in SuRF)
///
/// SuRF parameters (what we map to):
/// - include_dense: Always true for text indexes
/// - sparse_dense_ratio: Fixed at 16 for good performance
/// - suffix_type: kNone, kHash, or kReal based on desired accuracy
/// - hash_suffix_len: Length of hash suffixes (0, 4, or 8)
/// - real_suffix_len: Length of real suffixes (0 or 8)
///
/// Mapping strategy:
/// - Larger filter sizes + more hash functions = lower false positive rate desired
/// - Lower false positive rate = more aggressive suffix storage in SuRF
static SurfFilterParameters
convertBloomToSurfParameters(size_t filter_size_bytes, size_t num_hash_functions, size_t /*random_seed*/) // seed not used in SuRF
{
    // Map Bloom filter size and hash functions to SuRF suffix configuration
    // Larger filter sizes and more hash functions generally indicate desire for lower false positive rates

    // Calculate approximate false positive rate based on Bloom filter parameters
    // This is a rough approximation: fp_rate ≈ (1 - e^(-k*n/m))^k
    // where k = hash functions, n = expected items, m = bits in filter

    // For text indexes, we use a heuristic based on filter size and hash functions
    if (filter_size_bytes >= 1024 && num_hash_functions >= 5)
    {
        // Largest filter with many hash functions -> very very low FP rate -> use real suffixes
        // approx_fp_rate = 0.001;
        return SurfFilterParameters(true, 16, kReal, 0, 8);
    }
    else if (filter_size_bytes >= 512 && num_hash_functions >= 4)
    {
        // Large filter with many hash functions -> very low FP rate -> use real suffixes
        // approx_fp_rate = 0.001;
        return SurfFilterParameters(true, 16, kReal, 0, 4);
    }
    else if (filter_size_bytes >= 256 && num_hash_functions >= 3)
    {
        // Medium-large filter -> low FP rate -> use hash suffixes
        // approx_fp_rate = 0.01;
        return SurfFilterParameters(true, 16, kHash, 4, 0);
    }
    else if (filter_size_bytes >= 128 && num_hash_functions >= 2)
    {
        // Medium filter -> medium FP rate -> use shorter hash suffixes
        // approx_fp_rate = 0.025;
        return SurfFilterParameters(true, 16, kHash, 2, 0);
    }
    else
    {
        // Small filter or few hash functions -> higher FP rate -> no suffixes
        // approx_fp_rate = 0.05;
        return SurfFilterParameters(true, 16, kNone, 0, 0);
    }
}

MergeTreeIndexPtr surfFilterIndexTextCreator(const IndexDescription & index)
{
    if (index.type == NgramTokenExtractor::getName2())
    {
        size_t n = index.arguments[0].safeGet<size_t>();
        size_t filter_size_bytes = index.arguments[1].safeGet<size_t>();
        size_t num_hash_functions = index.arguments[2].safeGet<size_t>();
        size_t random_seed = index.arguments[3].safeGet<size_t>();


        // Convert Bloom filter parameters to SuRF parameters
        SurfFilterParameters params = convertBloomToSurfParameters(filter_size_bytes, num_hash_functions, random_seed);

        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);

        return std::make_shared<MergeTreeIndexSurfFilterText>(index, params, std::move(tokenizer));
    }
    if (index.type == DefaultTokenExtractor::getName2())
    {
        size_t filter_size_bytes = index.arguments[0].safeGet<size_t>();
        size_t num_hash_functions = index.arguments[1].safeGet<size_t>();
        size_t random_seed = index.arguments[2].safeGet<size_t>();


        // Convert Bloom filter parameters to SuRF parameters
        SurfFilterParameters params = convertBloomToSurfParameters(filter_size_bytes, num_hash_functions, random_seed);

        auto tokenizer = std::make_unique<DefaultTokenExtractor>();

        return std::make_shared<MergeTreeIndexSurfFilterText>(index, params, std::move(tokenizer));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknownnn index type: {}", backQuote(index.name));
}

void surfFilterIndexTextValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & array_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(array_type.getNestedType());
        }
        else if (data_type.isLowCardinality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString() && !data_type.isIPv6())
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Ngram and token surf filter indexes can only be used with column types `String`, `FixedString`, "
                "`LowCardinality(String)`, "
                "`LowCardinality(FixedString)`, `Array(String)` or `Array(FixedString)`");
    }

    if (index.type == NgramTokenExtractor::getName2())
    {
        if (index.arguments.size() != 4)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "`ngramsf_v1` index must have exactly 4 arguments: n, filter_size_bytes, num_hash_functions, random_seed.");
    }
    else if (index.type == DefaultTokenExtractor::getName2())
    {
        if (index.arguments.size() != 3)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "`tokensf_v1` index must have exactly 3 arguments: filter_size_bytes, num_hash_functions, random_seed.");
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index type: {}", backQuote(index.name));
    }

    assert(index.arguments.size() >= 3);

    for (const auto & arg : index.arguments)
        if (arg.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to *bf_v2 index must be unsigned integers");

    // Validate parameter ranges for SuRF conversion
    if (index.type == NgramTokenExtractor::getName2())
    {
        size_t n = index.arguments[0].safeGet<size_t>();
        size_t filter_size_bytes = index.arguments[1].safeGet<size_t>();
        size_t num_hash_functions = index.arguments[2].safeGet<size_t>();
        // size_t random_seed = index.arguments[3].safeGet<size_t>(); // Not used in validation

        if (n < 2 || n > 8)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngram size must be between 2 and 8, got {}", n);

        if (filter_size_bytes < 64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size must be at least 64 bytes, got {}", filter_size_bytes);

        if (num_hash_functions == 0 || num_hash_functions > 16)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of hash functions must be between 1 and 16, got {}", num_hash_functions);
    }
    else if (index.type == DefaultTokenExtractor::getName2())
    {
        size_t filter_size_bytes = index.arguments[0].safeGet<size_t>();
        size_t num_hash_functions = index.arguments[1].safeGet<size_t>();
        // size_t random_seed = index.arguments[2].safeGet<size_t>(); // Not used in validation

        if (filter_size_bytes < 64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size must be at least 64 bytes, got {}", filter_size_bytes);

        if (num_hash_functions == 0 || num_hash_functions > 16)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of hash functions must be between 1 and 16, got {}", num_hash_functions);
    }
}

}
