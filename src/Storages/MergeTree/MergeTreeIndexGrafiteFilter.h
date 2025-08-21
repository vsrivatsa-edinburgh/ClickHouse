#pragma once

#include <cstddef>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/GrafiteFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

class Set;
using ConstSetPtr = std::shared_ptr<const Set>;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class MergeTreeIndexGranuleGrafiteFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleGrafiteFilter(size_t index_columns_, double bits_per_key = 2.0);

    MergeTreeIndexGranuleGrafiteFilter(const std::vector<std::set<std::string>> & column_keys, double bits_per_key = 2.0);

    bool empty() const override;

    size_t memoryUsageBytes() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<GrafiteFilterPtr> & getFilters() const { return grafite_filters; }
    void setFilters(const std::vector<GrafiteFilterPtr> & filters) { grafite_filters = filters; }
    void setTotalRows(size_t rows) { total_rows = rows; }

private:
    size_t total_rows = 0;
    double bits_per_key;
    std::vector<GrafiteFilterPtr> grafite_filters;

    void fillingGrafiteFilterWithKeys(GrafiteFilterPtr & bf, const std::set<std::string> & keys, double bits_per_key_param) const;
};

class MergeTreeIndexConditionGrafiteFilter final : public IMergeTreeIndexCondition, WithContext
{
public:
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_HAS_ANY,
            FUNCTION_HAS_ALL,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            /// Range functions for Grafite
            FUNCTION_GREATER,
            FUNCTION_GREATER_OR_EQUALS,
            FUNCTION_LESS,
            FUNCTION_LESS_OR_EQUALS,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN)
            : function(function_)
        {
        } /// NOLINT

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    MergeTreeIndexConditionGrafiteFilter(const ActionsDAG::Node * predicate, ContextPtr context_, const Block & header_);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override
    {
        if (const auto & bf_granule = typeid_cast<const MergeTreeIndexGranuleGrafiteFilter *>(granule.get()))
            return mayBeTrueOnGranule(bf_granule);

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requires grafite filter index granule.");
    }

private:
    const Block & header;
    std::vector<RPNElement> rpn;

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleGrafiteFilter * granule) const;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseFunction(const RPNBuilderTreeNode & node, RPNElement & out, const RPNBuilderTreeNode * parent);

    bool traverseTreeIn(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const ConstSetPtr & prepared_set,
        const DataTypePtr & type,
        const ColumnPtr & column,
        RPNElement & out);

    bool traverseTreeEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out,
        const RPNBuilderTreeNode * parent);
};

class MergeTreeIndexAggregatorGrafiteFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorGrafiteFilter(const Names & columns_name_, double bits_per_key = 2.0);

    bool empty() const override;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    const Names index_columns_name;

    // Grafite filters for incremental insertion
    std::vector<GrafiteFilterPtr> grafite_filters;
    // Accumulate keys for sorting before insertion
    std::vector<std::vector<std::string>> accumulated_keys;
    double bits_per_key = 2.0;
    size_t total_rows = 0;
};


class MergeTreeIndexGrafiteFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexGrafiteFilter(const IndexDescription & index_, double bits_per_key = 2.0);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

private:
    double bits_per_key = 2.0;
};
}
