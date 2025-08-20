#include <Storages/MergeTree/MergeTreeIndexSurfFilter.h>

#include <iostream>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/SurfFilterHash.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <fmt/format.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <base/unaligned.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <string>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_QUERY;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
}

// Forward declarations for key extraction functions
std::string extractKeyFromField(const Field & field, const DataTypePtr & data_type);
std::vector<std::string> extractKeysFromColumn(const ColumnPtr & column, const DataTypePtr & data_type, size_t pos, size_t limit);

// Convert false positive probability to SuRF parameters
static SurfFilterParameters getSurfParameters(int variant)
{
    // For SuRF, we can configure parameters based on false positive probability
    // Lower false positive probability = more aggressive suffix storage

    if (variant == 0) // Low FP rate
    {
        // Use hash suffixes with more suffix bits for maximum accuracy
        return SurfFilterParameters(true, 16, kHash, 4, 0);
    }
    else if (variant == 1) // Very low FP rate
    {
        // Use shorter hash suffixes
        return SurfFilterParameters(true, 16, kHash, 8, 0);
    }
    else if (variant == 2) // High FP rate (0.025)
    {
        // Use real suffixes for better performance
        return SurfFilterParameters(true, 16, kReal, 0, 8);
    }
    else // Very high FP rate (> 0.05)
    {
        // No suffixes for maximum speed
        return SurfFilterParameters(true, 16, kNone, 0, 0);
    }
}

MergeTreeIndexGranuleSurfFilter::MergeTreeIndexGranuleSurfFilter(size_t index_columns_, int variant)
    : surf_filters(index_columns_)
{
    total_rows = 0;
    // Create SurfFilter with user-provided parameters
    SurfFilterParameters params = getSurfParameters(variant);

    for (size_t column = 0; column < index_columns_; ++column)
        surf_filters[column] = std::make_shared<SurfFilter>(params);
}

MergeTreeIndexGranuleSurfFilter::MergeTreeIndexGranuleSurfFilter(const std::vector<std::set<std::string>> & column_keys_, int variant)
    : surf_filters(column_keys_.size())
{
    if (column_keys_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column keys empty or total_rows is zero.");

    size_t surf_filter_max_size = 0;
    for (const auto & column_key_set : column_keys_)
        surf_filter_max_size = std::max(surf_filter_max_size, column_key_set.size());

    total_rows = surf_filter_max_size;

    // Create SurfFilter with user-provided parameters
    SurfFilterParameters params = getSurfParameters(variant);

    for (size_t column = 0, columns = column_keys_.size(); column < columns; ++column)
    {
        surf_filters[column] = std::make_shared<SurfFilter>(params);
        fillingSurfFilterWithKeys(surf_filters[column], column_keys_[column], variant);
    }
}

bool MergeTreeIndexGranuleSurfFilter::empty() const
{
    return !total_rows;
}

size_t MergeTreeIndexGranuleSurfFilter::memoryUsageBytes() const
{
    size_t sum = 0;
    for (const auto & surf_filter : surf_filters)
        sum += surf_filter->memoryUsageBytes();
    return sum;
}

void MergeTreeIndexGranuleSurfFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    readVarUInt(total_rows, istr);

    // Deserialize each SurfFilter
    for (auto & filter : surf_filters)
    {
        if (!filter)
        {
            SurfFilterParameters params = getSurfParameters(1); // Use default false positive probability
            filter = std::make_shared<SurfFilter>(params);
        }
        filter->deserialize(istr);
    }
}

void MergeTreeIndexGranuleSurfFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty surf filter index.");

    writeVarUInt(total_rows, ostr);

    // Serialize each SurfFilter
    for (const auto & surf_filter : surf_filters)
    {
        surf_filter->serialize(ostr);
    }
}

void MergeTreeIndexGranuleSurfFilter::fillingSurfFilterWithKeys(
    SurfFilterPtr & surf_filter, const std::set<std::string> & keys, int variant) const
{
    if (keys.empty())
        return;

    // Convert std::set to std::vector for SuRF construction (keys are already sorted)
    std::vector<std::string> keys_vector(keys.begin(), keys.end());

    // Create SuRF with the sorted keys
    SurfFilterParameters params = getSurfParameters(variant);
    *surf_filter = SurfFilter(keys_vector, params);
}

namespace
{

ColumnWithTypeAndName getPreparedSetInfo(const ConstSetPtr & prepared_set)
{
    if (prepared_set->getDataTypes().size() == 1)
        return {prepared_set->getSetElements()[0], prepared_set->getElementsTypes()[0], "dummy"};

    Columns set_elements;
    for (auto & set_element : prepared_set->getSetElements())

        set_elements.emplace_back(set_element->convertToFullColumnIfConst());

    return {ColumnTuple::create(set_elements), std::make_shared<DataTypeTuple>(prepared_set->getElementsTypes()), "dummy"};
}

bool hashMatchesFilter(const SurfFilterPtr & surf_filter, UInt64 hash)
{
    // Convert hash to key for SurfFilter lookup
    // For now, use string representation of hash as temporary bridge
    std::string key = std::to_string(hash);
    return surf_filter->lookupKey(key);
}

bool keyMatchesFilter(const SurfFilterPtr & surf_filter, const std::string & key)
{
    // Direct key lookup in SuRF filter
    return surf_filter->lookupKey(key);
}

bool keyMatchesRangeFilter(
    const SurfFilterPtr & surf_filter, const std::string & key, MergeTreeIndexConditionSurfFilter::RPNElement::Function function)
{
    // Use concrete bounds instead of empty strings for better compatibility
    // For numeric data, use reasonable min/max bounds that cover typical ranges
    const std::string MIN_BOUND = ""; // Int32 min
    const std::string MAX_BOUND = "\uffff"; // UInt32 max as upper bound

    LOG_TRACE(&Poco::Logger::get("SurfFilter"), "keyMatchesRangeFilter called: key='{}', function={}", key, static_cast<int>(function));

    bool result = false;
    // For range operations, we use SuRF's range query capabilities
    switch (function)
    {
        case MergeTreeIndexConditionSurfFilter::RPNElement::FUNCTION_GREATER:
            // x > key: Look for anything in range (key, MAX_BOUND]
            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "FUNCTION_GREATER: lookupRange('{}', false, '{}', true)", key, MAX_BOUND);
            result = surf_filter->lookupRange(key, false, MAX_BOUND, true);
            break;
        case MergeTreeIndexConditionSurfFilter::RPNElement::FUNCTION_GREATER_OR_EQUALS:
            // x >= key: Look for anything in range [key, MAX_BOUND]
            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "FUNCTION_GREATER_OR_EQUALS: lookupRange('{}', true, '{}', true)", key, MAX_BOUND);
            result = surf_filter->lookupRange(key, true, MAX_BOUND, true);
            break;
        case MergeTreeIndexConditionSurfFilter::RPNElement::FUNCTION_LESS:
            // x < key: Look for anything in range [MIN_BOUND, key)
            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "FUNCTION_LESS: lookupRange('{}', true, '{}', false)", MIN_BOUND, key);
            result = surf_filter->lookupRange(MIN_BOUND, true, key, false);
            break;
        case MergeTreeIndexConditionSurfFilter::RPNElement::FUNCTION_LESS_OR_EQUALS:
            // x <= key: Look for anything in range [MIN_BOUND, key]
            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "FUNCTION_LESS_OR_EQUALS: lookupRange('{}', true, '{}', true)", MIN_BOUND, key);
            result = surf_filter->lookupRange(MIN_BOUND, true, key, true);
            break;
        default:
            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Unknown function: {}", static_cast<int>(function));
            result = false;
    }

    LOG_TRACE(&Poco::Logger::get("SurfFilter"), "keyMatchesRangeFilter result: {}", result);
    return result;
}

// bool maybeTrueOnSurfFilterWithKeys(const IColumn * column, const SurfFilterPtr & surf_filter, const DataTypePtr & data_type, bool match_all)
// {
//     const auto * const_column = typeid_cast<const ColumnConst *>(column);

//     if (const_column)
//     {
//         // Single constant value
//         Field field = const_column->getField();
//         std::string key = extractKeyFromField(field, data_type);
//         return keyMatchesFilter(surf_filter, key);
//     }

//     // Multiple values - extract keys from the column
//     // Create a temporary ColumnPtr by cloning the column since we need a proper ColumnPtr
//     ColumnPtr column_ptr = column->cloneResized(column->size());
//     auto keys = extractKeysFromColumn(column_ptr, data_type, 0, column->size());

//     if (match_all)
//     {
//         return std::all_of(keys.begin(), keys.end(), [&](const std::string & key) { return keyMatchesFilter(surf_filter, key); });
//     }

//     return std::any_of(keys.begin(), keys.end(), [&](const std::string & key) { return keyMatchesFilter(surf_filter, key); });
// }

bool maybeTrueOnSurfFilter(const IColumn * hash_column, const SurfFilterPtr & surf_filter, bool match_all)
{
    const auto * const_column = typeid_cast<const ColumnConst *>(hash_column);
    const auto * non_const_column = typeid_cast<const ColumnUInt64 *>(hash_column);

    if (!const_column && !non_const_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash column must be Const or UInt64.");

    if (const_column)
    {
        return hashMatchesFilter(surf_filter, const_column->getValue<UInt64>());
    }

    const ColumnUInt64::Container & hashes = non_const_column->getData();

    if (match_all)
    {
        return std::all_of(hashes.begin(), hashes.end(), [&](const auto & hash_row) { return hashMatchesFilter(surf_filter, hash_row); });
    }

    return std::any_of(hashes.begin(), hashes.end(), [&](const auto & hash_row) { return hashMatchesFilter(surf_filter, hash_row); });
}

}

// Simple key extraction functions for SuRF
std::string extractKeyFromField(const Field & field, const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (field.isNull())
        return "0"; // Simple null representation

    if (which.isString() || which.isFixedString())
        return field.safeGet<String>();

    // Simple string format for all numeric types
    if (which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64() || which.isUInt128() || which.isUInt256())
    {
        return toString(field.safeGet<UInt64>());
    }
    if (which.isInt8() || which.isInt16() || which.isInt32() || which.isInt64() || which.isInt128() || which.isInt256())
    {
        return toString(field.safeGet<Int64>());
    }
    if (which.isEnum8() || which.isEnum16() || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64())
    {
        return toString(field.safeGet<UInt64>());
    }
    if (which.isFloat32() || which.isFloat64())
    {
        return toString(field.safeGet<Float64>());
    }
    if (which.isUUID() || which.isIPv4() || which.isIPv6())
    {
        return field.dump();
    }

    // For other types, convert to string representation
    return field.dump();
}

std::vector<std::string> extractKeysFromColumn(const ColumnPtr & column, const DataTypePtr & data_type, size_t pos, size_t limit)
{
    std::vector<std::string> keys;
    keys.reserve(limit);

    WhichDataType which(data_type);

    if (which.isString())
    {
        const auto * string_col = typeid_cast<const ColumnString *>(column.get());
        if (string_col)
        {
            for (size_t i = pos; i < pos + limit; ++i)
            {
                StringRef str_ref = string_col->getDataAt(i);
                keys.emplace_back(str_ref.data, str_ref.size);
            }
            return keys;
        }
    }

    if (which.isFixedString())
    {
        const auto * fixed_string_col = typeid_cast<const ColumnFixedString *>(column.get());
        if (fixed_string_col)
        {
            for (size_t i = pos; i < pos + limit; ++i)
            {
                StringRef str_ref = fixed_string_col->getDataAt(i);
                keys.emplace_back(str_ref.data, str_ref.size);
            }
            return keys;
        }
    }

    if (which.isUInt64())
    {
        const auto * uint64_col = typeid_cast<const ColumnUInt64 *>(column.get());
        if (uint64_col)
        {
            for (size_t i = pos; i < pos + limit; ++i)
            {
                UInt64 big_endian_value;
                unalignedStoreBigEndian<UInt64>(&big_endian_value, uint64_col->getElement(i));
                keys.emplace_back(reinterpret_cast<const char *>(&big_endian_value), sizeof(big_endian_value));
            }
            return keys;
        }
    }

    // Fallback: extract field by field
    for (size_t i = pos; i < pos + limit; ++i)
    {
        Field field;
        column->get(i, field);
        keys.push_back(extractKeyFromField(field, data_type));
    }

    return keys;
}

MergeTreeIndexConditionSurfFilter::MergeTreeIndexConditionSurfFilter(
    const ActionsDAG::Node * predicate, ContextPtr context_, const Block & header_)
    : WithContext(context_)
    , header(header_)
{
    if (!predicate)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate, context_, [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeIndexConditionSurfFilter::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_EQUALS,
         RPNElement::FUNCTION_NOT_EQUALS,
         RPNElement::FUNCTION_HAS,
         RPNElement::FUNCTION_HAS_ANY,
         RPNElement::FUNCTION_HAS_ALL,
         RPNElement::FUNCTION_IN,
         RPNElement::FUNCTION_NOT_IN,
         RPNElement::FUNCTION_GREATER,
         RPNElement::FUNCTION_GREATER_OR_EQUALS,
         RPNElement::FUNCTION_LESS,
         RPNElement::FUNCTION_LESS_OR_EQUALS});
}

bool MergeTreeIndexConditionSurfFilter::mayBeTrueOnGranule(const MergeTreeIndexGranuleSurfFilter * granule) const
{
    std::vector<BoolMask> rpn_stack;
    const auto & filters = granule->getFilters();

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (
            element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_HAS || element.function == RPNElement::FUNCTION_HAS_ANY
            || element.function == RPNElement::FUNCTION_HAS_ALL || element.function == RPNElement::FUNCTION_GREATER
            || element.function == RPNElement::FUNCTION_GREATER_OR_EQUALS || element.function == RPNElement::FUNCTION_LESS
            || element.function == RPNElement::FUNCTION_LESS_OR_EQUALS)
        {
            bool match_rows = false;  // Start with false for OR operations (IN), true for AND operations (HAS_ALL)
            bool match_all = element.function == RPNElement::FUNCTION_HAS_ALL;
            bool is_in_operation = element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN;
            
            if (match_all)
                match_rows = true;  // For AND operations, start with true
            
            const auto & predicate = element.predicate;
            
            for (size_t index = 0; index < predicate.size(); ++index)
            {
                const auto & query_index_hash = predicate[index];
                const auto & filter = filters[query_index_hash.first];
                const ColumnPtr & key_column = query_index_hash.second;

                bool current_match = false;

                // Extract key from the key_column
                const auto * string_column = typeid_cast<const ColumnConst *>(key_column.get());
                if (string_column)
                {
                    const auto * inner_string = typeid_cast<const ColumnString *>(&string_column->getDataColumn());
                    if (inner_string && inner_string->size() > 0)
                    {
                        std::string key = inner_string->getDataAt(0).toString();

                        LOG_TRACE(
                            &Poco::Logger::get("SurfFilter"),
                            "Processing predicate: key='{}', function={}",
                            key,
                            static_cast<int>(element.function));

                        // Use range filtering for range operations, exact matching for others
                        if (element.function == RPNElement::FUNCTION_GREATER || element.function == RPNElement::FUNCTION_GREATER_OR_EQUALS
                            || element.function == RPNElement::FUNCTION_LESS || element.function == RPNElement::FUNCTION_LESS_OR_EQUALS)
                        {
                            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Using range filtering for key='{}'", key);
                            current_match = keyMatchesRangeFilter(filter, key, element.function);
                        }
                        else
                        {
                            LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Using exact matching for key='{}'", key);
                            current_match = keyMatchesFilter(filter, key);
                        }

                        LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Match result for key='{}': {}", key, current_match);
                    }
                    else
                    {
                        current_match = maybeTrueOnSurfFilter(&*key_column, filter, match_all);
                    }
                }
                else
                {
                    current_match = maybeTrueOnSurfFilter(&*key_column, filter, match_all);
                }
                
                // Update match_rows based on operation type
                if (match_all)
                {
                    // AND operation: all must match
                    match_rows = match_rows && current_match;
                    if (!match_rows)
                        break;  // Early exit for AND operations
                }
                else
                {
                    // OR operation: any can match
                    match_rows = match_rows || current_match;
                    if (match_rows && is_in_operation)
                        break;  // Early exit for IN operations when we find a match
                }
            }

            rpn_stack.emplace_back(match_rows, true);
            if (element.function == RPNElement::FUNCTION_NOT_EQUALS || element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueInRange");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionSurfFilter::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
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

    return traverseFunction(node, out, nullptr /*parent*/);
}

bool MergeTreeIndexConditionSurfFilter::traverseFunction(
    const RPNBuilderTreeNode & node, RPNElement & out, const RPNBuilderTreeNode * parent)
{
    if (!node.isFunction())
        return false;

    const auto function = node.toFunctionNode();
    auto arguments_size = function.getArgumentsSize();
    auto function_name = function.getFunctionName();

    if (parent == nullptr)
    {
        /// Recurse a little bit for indexOf().
        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function.getArgumentAt(i);
            if (traverseFunction(argument, out, &node))
                return true;
        }
    }

    if (arguments_size != 2)
        return false;

    /// indexOf() should be inside comparison function, e.g. greater(indexOf(key, 42), 0).
    /// Other conditions should be at top level, e.g. equals(key, 42), not equals(equals(key, 42), 1).
    if ((function_name == "indexOf") != (parent != nullptr))
        return false;

    auto lhs_argument = function.getArgumentAt(0);
    auto rhs_argument = function.getArgumentAt(1);

    if (functionIsInOrGlobalInOperator(function_name))
    {
        if (auto future_set = rhs_argument.tryGetPreparedSet(); future_set)
        {
            if (auto prepared_set = future_set->buildOrderedSetInplace(rhs_argument.getTreeContext().getQueryContext()); prepared_set)
            {
                if (prepared_set->hasExplicitSetElements())
                {
                    const auto prepared_info = getPreparedSetInfo(prepared_set);
                    if (traverseTreeIn(function_name, lhs_argument, prepared_set, prepared_info.type, prepared_info.column, out))
                        return true;
                }
            }
        }
        return false;
    }

    if (function_name == "equals" || function_name == "notEquals" || function_name == "has" || function_name == "mapContains"
        || function_name == "mapContainsKey" || function_name == "mapContainsValue" || function_name == "indexOf"
        || function_name == "hasAny" || function_name == "hasAll" || function_name == "greater" || function_name == "greaterOrEquals"
        || function_name == "less" || function_name == "lessOrEquals")
    {
        Field const_value;
        DataTypePtr const_type;

        if (rhs_argument.tryGetConstant(const_value, const_type))
        {
            if (traverseTreeEquals(function_name, lhs_argument, const_type, const_value, out, parent))
                return true;
        }
        else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
        {
            if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, out, parent))
                return true;
        }

        return false;
    }

    return false;
}

bool MergeTreeIndexConditionSurfFilter::traverseTreeIn(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const ConstSetPtr & prepared_set,
    const DataTypePtr & type,
    const ColumnPtr & column,
    RPNElement & out)
{
    auto key_node_column_name = key_node.getColumnName();

    if (header.has(key_node_column_name))
    {
        size_t row_size = column->size();
        size_t position = header.getPositionByName(key_node_column_name);
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, index_type);
        
        // For IN operations, we need to create key columns for each value in the set
        if (function_name == "in" || function_name == "globalIn" || function_name == "notIn" || function_name == "globalNotIn")
        {
            // Handle each value in the IN clause separately
            for (size_t i = 0; i < row_size; ++i)
            {
                Field field;
                converted_column->get(i, field);
                auto key_column = SurfFilterHash::keyWithField(index_type.get(), field);
                out.predicate.emplace_back(std::make_pair(position, key_column));
            }
        }
        else
        {
            // For non-IN operations, use the original hash-based approach
            out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::hashWithColumn(index_type, converted_column, 0, row_size)));
        }

        if (function_name == "in" || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        if (function_name == "notIn" || function_name == "globalNotIn")
            out.function = RPNElement::FUNCTION_NOT_IN;

        return true;
    }

    if (key_node.isFunction())
    {
        auto key_node_function = key_node.toFunctionNode();
        auto key_node_function_name = key_node_function.getFunctionName();
        size_t key_node_function_arguments_size = key_node_function.getArgumentsSize();

        WhichDataType which(type);

        if (which.isTuple() && key_node_function_name == "tuple")
        {
            const auto & tuple_column = typeid_cast<const ColumnTuple *>(column.get());
            const auto & tuple_data_type = typeid_cast<const DataTypeTuple *>(type.get());

            if (tuple_data_type->getElements().size() != key_node_function_arguments_size
                || tuple_column->getColumns().size() != key_node_function_arguments_size)
                return false;

            bool match_with_subtype = false;
            const auto & sub_columns = tuple_column->getColumns();
            const auto & sub_data_types = tuple_data_type->getElements();

            for (size_t index = 0; index < key_node_function_arguments_size; ++index)
                match_with_subtype |= traverseTreeIn(
                    function_name, key_node_function.getArgumentAt(index), nullptr, sub_data_types[index], sub_columns[index], out);

            return match_with_subtype;
        }

        if (key_node_function_name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] IN ('') because if the key does not exist in the map
              * we return the default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where the map key does not exist.
              */
            if (!prepared_set)
                return false;

            auto default_column_to_check = type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
            ColumnWithTypeAndName default_column_with_type_to_check{default_column_to_check, type, ""};
            ColumnsWithTypeAndName default_columns_with_type_to_check = {default_column_with_type_to_check};
            auto set_contains_default_value_predicate_column
                = prepared_set->execute(default_columns_with_type_to_check, false /*negative*/);
            const auto & set_contains_default_value_predicate_column_typed
                = assert_cast<const ColumnUInt8 &>(*set_contains_default_value_predicate_column);
            bool set_contain_default_value = set_contains_default_value_predicate_column_typed.getData()[0];
            if (set_contain_default_value)
                return false;

            auto first_argument = key_node_function.getArgumentAt(0);
            const auto column_name = first_argument.getColumnName();
            auto map_keys_index_column_name = fmt::format("mapKeys({})", column_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", column_name);

            if (header.has(map_keys_index_column_name))
            {
                /// For mapKeys we serialize key argument with surf filter

                auto second_argument = key_node_function.getArgumentAt(1);

                Field constant_value;
                DataTypePtr constant_type;

                if (second_argument.tryGetConstant(constant_value, constant_type))
                {
                    size_t position = header.getPositionByName(map_keys_index_column_name);
                    const DataTypePtr & index_type = header.getByPosition(position).type;
                    const DataTypePtr actual_type = SurfFilter::getPrimitiveType(index_type);
                    out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::keyWithField(actual_type.get(), constant_value)));
                }
                else
                {
                    return false;
                }
            }
            else if (header.has(map_values_index_column_name))
            {
                /// For mapValues we serialize set with surf filter

                size_t row_size = column->size();
                size_t position = header.getPositionByName(map_values_index_column_name);
                const DataTypePtr & index_type = header.getByPosition(position).type;
                const auto & array_type = assert_cast<const DataTypeArray &>(*index_type);
                const auto & array_nested_type = array_type.getNestedType();
                const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, array_nested_type);
                
                // For IN operations, create key columns for each value
                if (function_name == "in" || function_name == "globalIn" || function_name == "notIn" || function_name == "globalNotIn")
                {
                    for (size_t i = 0; i < row_size; ++i)
                    {
                        Field field;
                        converted_column->get(i, field);
                        auto key_column = SurfFilterHash::keyWithField(array_nested_type.get(), field);
                        out.predicate.emplace_back(std::make_pair(position, key_column));
                    }
                }
                else
                {
                    out.predicate.emplace_back(
                        std::make_pair(position, SurfFilterHash::hashWithColumn(array_nested_type, converted_column, 0, row_size)));
                }
            }
            else
            {
                return false;
            }

            if (function_name == "in" || function_name == "globalIn")
                out.function = RPNElement::FUNCTION_IN;

            if (function_name == "notIn" || function_name == "globalNotIn")
                out.function = RPNElement::FUNCTION_NOT_IN;

            return true;
        }
    }

    return false;
}


static bool indexOfCanUseSurfFilter(const RPNBuilderTreeNode * parent)
{
    if (!parent)
        return true;

    if (!parent->isFunction())
        return false;

    auto function = parent->toFunctionNode();
    auto function_name = function.getFunctionName();

    /// `parent` is a function where `indexOf` is located.
    /// Example: `indexOf(arr, x) = 1`, parent is a function named `equals`.
    if (function_name == "and")
    {
        return true;
    }
    if (function_name == "equals" /// notEquals is not applicable
        || function_name == "greater" || function_name == "greaterOrEquals" || function_name == "less" || function_name == "lessOrEquals")
    {
        size_t function_arguments_size = function.getArgumentsSize();
        if (function_arguments_size != 2)
            return false;

        /// We don't allow constant expressions like `indexOf(arr, x) = 1 + 0` but it's negligible.

        /// We should return true when the corresponding expression implies that the array contains the element.
        /// Example: when `indexOf(arr, x)` > 10 is written, it means that arr definitely should contain the element
        /// (at least at 11th position but it does not matter).

        bool reversed = false;
        Field constant_value;
        DataTypePtr constant_type;

        if (function.getArgumentAt(0).tryGetConstant(constant_value, constant_type))
        {
            reversed = true;
        }
        else if (function.getArgumentAt(1).tryGetConstant(constant_value, constant_type))
        {
        }
        else
        {
            return false;
        }

        Field zero(0);
        bool constant_equal_zero = accurateEquals(constant_value, zero);

        if (function_name == "equals" && !constant_equal_zero)
        {
            /// indexOf(...) = c, c != 0
            return true;
        }
        if (function_name == "notEquals" && constant_equal_zero)
        {
            /// indexOf(...) != c, c = 0
            return true;
        }
        if (function_name == (reversed ? "less" : "greater") && !accurateLess(constant_value, zero))
        {
            /// indexOf(...) > c, c >= 0
            return true;
        }
        if (function_name == (reversed ? "lessOrEquals" : "greaterOrEquals") && accurateLess(zero, constant_value))
        {
            /// indexOf(...) >= c, c > 0
            return true;
        }

        return false;
    }

    return false;
}


bool MergeTreeIndexConditionSurfFilter::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out,
    const RPNBuilderTreeNode * parent)
{
    auto key_column_name = key_node.getColumnName();

    if (header.has(key_column_name))
    {
        size_t position = header.getPositionByName(key_column_name);
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (function_name == "has" || function_name == "indexOf")
        {
            if (!array_type)
                return false;

            /// We can treat `indexOf` function similar to `has`.
            /// But it is little more cumbersome, compare: `has(arr, elem)` and `indexOf(arr, elem) != 0`.
            /// The `parent` in this context is expected to be function `!=` (`notEquals`).
            if (function_name == "has" || indexOfCanUseSurfFilter(parent))
            {
                out.function = RPNElement::FUNCTION_HAS;
                const DataTypePtr actual_type = SurfFilter::getPrimitiveType(array_type->getNestedType());
                auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
                if (converted_field.isNull())
                    return false;

                out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::keyWithField(actual_type.get(), converted_field)));
            }
        }
        else if (function_name == "hasAny" || function_name == "hasAll")
        {
            if (!array_type)
                return false;

            if (value_field.getType() != Field::Types::Array)
                return false;

            const DataTypePtr actual_type = SurfFilter::getPrimitiveType(array_type->getNestedType());
            ColumnPtr column;
            {
                const bool is_nullable = actual_type->isNullable();
                auto mutable_column = actual_type->createColumn();

                for (const auto & f : value_field.safeGet<Array>())
                {
                    if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType())) /// NOLINT(readability-static-accessed-through-instance)
                        return false;

                    auto converted = convertFieldToType(f, *actual_type);
                    if (converted.isNull())
                        return false;

                    mutable_column->insert(converted);
                }

                column = std::move(mutable_column);
            }

            out.function = function_name == "hasAny" ? RPNElement::FUNCTION_HAS_ANY : RPNElement::FUNCTION_HAS_ALL;
            out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::hashWithColumn(actual_type, column, 0, column->size())));
        }
        else
        {
            if (array_type)
                return false;

            // Map function names to RPNElement::Function enums
            if (function_name == "equals")
                out.function = RPNElement::FUNCTION_EQUALS;
            else if (function_name == "notEquals")
                out.function = RPNElement::FUNCTION_NOT_EQUALS;
            else if (function_name == "greater")
                out.function = RPNElement::FUNCTION_GREATER;
            else if (function_name == "greaterOrEquals")
                out.function = RPNElement::FUNCTION_GREATER_OR_EQUALS;
            else if (function_name == "less")
                out.function = RPNElement::FUNCTION_LESS;
            else if (function_name == "lessOrEquals")
                out.function = RPNElement::FUNCTION_LESS_OR_EQUALS;
            else
                return false;

            const DataTypePtr actual_type = SurfFilter::getPrimitiveType(index_type);
            auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
            if (converted_field.isNull())
                return false;

            out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::keyWithField(actual_type.get(), converted_field)));
        }

        return true;
    }

    if (function_name == "mapContainsValue" || function_name == "mapContainsKey" || function_name == "mapContains"
        || function_name == "has")
    {
        auto map_keys_index_column_name = fmt::format("mapKeys({})", key_column_name);
        if (function_name == "mapContainsValue")
            map_keys_index_column_name = fmt::format("mapValues({})", key_column_name);

        if (!header.has(map_keys_index_column_name))
            return false;

        size_t position = header.getPositionByName(map_keys_index_column_name);

        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (!array_type)
            return false;

        out.function = RPNElement::FUNCTION_HAS;
        const DataTypePtr actual_type = SurfFilter::getPrimitiveType(array_type->getNestedType());
        auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
        if (converted_field.isNull())
            return false;

        out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::keyWithField(actual_type.get(), converted_field)));
        return true;
    }

    if (key_node.isFunction())
    {
        WhichDataType which(value_type);

        auto key_node_function = key_node.toFunctionNode();
        auto key_node_function_name = key_node_function.getFunctionName();
        size_t key_node_function_arguments_size = key_node_function.getArgumentsSize();

        if (which.isTuple() && key_node_function_name == "tuple")
        {
            const Tuple & tuple = value_field.safeGet<Tuple>();
            const auto * value_tuple_data_type = typeid_cast<const DataTypeTuple *>(value_type.get());

            if (tuple.size() != key_node_function_arguments_size)
                return false;

            bool match_with_subtype = false;
            const DataTypes & subtypes = value_tuple_data_type->getElements();

            for (size_t index = 0; index < tuple.size(); ++index)
                match_with_subtype |= traverseTreeEquals(
                    function_name, key_node_function.getArgumentAt(index), subtypes[index], tuple[index], out, &key_node);

            return match_with_subtype;
        }

        if (key_node_function_name == "arrayElement" && (function_name == "equals" || function_name == "notEquals"))
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

            auto first_argument = key_node_function.getArgumentAt(0);
            const auto column_name = first_argument.getColumnName();

            auto map_keys_index_column_name = fmt::format("mapKeys({})", column_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", column_name);

            size_t position = 0;
            Field const_value = value_field;
            DataTypePtr const_type;

            if (header.has(map_keys_index_column_name))
            {
                position = header.getPositionByName(map_keys_index_column_name);
                auto second_argument = key_node_function.getArgumentAt(1);

                if (!second_argument.tryGetConstant(const_value, const_type))
                    return false;
            }
            else if (header.has(map_values_index_column_name))
            {
                position = header.getPositionByName(map_values_index_column_name);
            }
            else
            {
                return false;
            }

            out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;

            const auto & index_type = header.getByPosition(position).type;
            const auto actual_type = SurfFilter::getPrimitiveType(index_type);
            out.predicate.emplace_back(std::make_pair(position, SurfFilterHash::keyWithField(actual_type.get(), const_value)));

            return true;
        }
    }

    return false;
}

MergeTreeIndexAggregatorSurfFilter::MergeTreeIndexAggregatorSurfFilter(const Names & columns_name_, int variant_)
    : index_columns_name(columns_name_)
    , surf_filters(columns_name_.size())
    , accumulated_keys(columns_name_.size())
    , variant(variant_)
{
    // We don't need to initialize SuRF filters here since we'll create them
    // directly from accumulated keys in getGranuleAndReset()
}

bool MergeTreeIndexAggregatorSurfFilter::empty() const
{
    return !total_rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSurfFilter::getGranuleAndReset()
{
    // Create new SuRF filters for the granule (separate from aggregator's working filters)
    std::vector<SurfFilterPtr> granule_filters(surf_filters.size());

    // Sort accumulated keys and create finalized filters for the granule
    for (size_t i = 0; i < surf_filters.size(); ++i)
    {
        if (accumulated_keys[i].empty())
        {
            // Create empty filter for this column
            SurfFilterParameters params = getSurfParameters(variant);
            granule_filters[i] = std::make_shared<SurfFilter>(params);
            continue;
        }

        // Sort all accumulated keys for this column
        std::sort(accumulated_keys[i].begin(), accumulated_keys[i].end());

        // Create a new SuRF filter for the granule and build it with sorted keys
        SurfFilterParameters params = getSurfParameters(variant);
        granule_filters[i] = std::make_shared<SurfFilter>(accumulated_keys[i], params);
    }

    // Create granule with the finalized filters
    auto granule = std::make_shared<MergeTreeIndexGranuleSurfFilter>(index_columns_name.size());
    granule->setFilters(granule_filters);
    granule->setTotalRows(total_rows);

    // Reset aggregator state for next granule
    total_rows = 0;
    for (size_t i = 0; i < surf_filters.size(); ++i)
    {
        accumulated_keys[i].clear();
    }

    return granule;
}

void MergeTreeIndexAggregatorSurfFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (size_t column = 0; column < index_columns_name.size(); ++column)
    {
        const auto & column_and_type = block.getByName(index_columns_name[column]);

        // Extract actual keys and accumulate them for later sorted insertion
        try
        {
            auto keys = extractKeysFromColumn(column_and_type.column, column_and_type.type, *pos, max_read_rows);

            // Accumulate keys for later sorting and insertion
            for (const auto & key : keys)
            {
                accumulated_keys[column].push_back(key);
                LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Accumulated key for column {}: '{}'", column, key);
            }
        }
        catch (...)
        {
            // Fallback: convert hashes to string keys
            auto index_column = SurfFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);
            const auto & index_col = checkAndGetColumn<ColumnUInt64>(*index_column);
            const auto & index_data = index_col.getData();

            // Accumulate hash keys
            for (const auto & hash : index_data)
            {
                std::string key = std::to_string(hash);
                accumulated_keys[column].push_back(key);
                LOG_TRACE(&Poco::Logger::get("SurfFilter"), "Accumulated hash key for column {}: '{}'", column, key);
            }
        }
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
}

MergeTreeIndexSurfFilter::MergeTreeIndexSurfFilter(const IndexDescription & index_, int variant_)
    : IMergeTreeIndex(index_)
    , variant(variant_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexSurfFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSurfFilter>(index.column_names.size(), variant);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSurfFilter::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorSurfFilter>(index.column_names, variant);
}

MergeTreeIndexConditionPtr MergeTreeIndexSurfFilter::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSurfFilter>(predicate, context, index.sample_block);
}

static void assertIndexColumnsType(const Block & header)
{
    if (!header || !header.columns())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index must have columns.");

    const DataTypes & columns_data_types = header.getDataTypes();

    for (const auto & type : columns_data_types)
    {
        const IDataType * actual_type = SurfFilter::getPrimitiveType(type).get();
        WhichDataType which(actual_type);

        if (!which.isUInt() && !which.isInt() && !which.isString() && !which.isFixedString() && !which.isFloat() && !which.isDate()
            && !which.isDateTime() && !which.isDateTime64() && !which.isEnum() && !which.isUUID() && !which.isIPv4() && !which.isIPv6())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type {} of surf filter index.", type->getName());
    }
}

MergeTreeIndexPtr surfFilterIndexCreator(const IndexDescription & index)
{
    int variant = 0; // Default variant

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];
        variant = std::min<int>(3, std::max<int>(argument.safeGet<int>(), 0)); // Allow 0-3 range
    }

    return std::make_shared<MergeTreeIndexSurfFilter>(index, variant);
}

void surfFilterIndexValidator(const IndexDescription & index, bool attach)
{
    assertIndexColumnsType(index.sample_block);

    if (index.arguments.size() > 1)
    {
        if (!attach) /// This is for backward compatibility.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SurfFilter index cannot have more than one parameter.");
    }

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];

        if (!attach)
        {
            int variant_value = argument.safeGet<int>();
            if (variant_value < 0 || variant_value > 3)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The SurfFilter variant must be an integer between 0 and 3.");
        }
    }
}

}
