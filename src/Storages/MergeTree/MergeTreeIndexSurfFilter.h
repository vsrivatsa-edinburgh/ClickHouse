#pragma once

#include <cstddef>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/SurfFilter.h>
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

class MergeTreeIndexGranuleSurfFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleSurfFilter(size_t index_columns_, double false_positive_probability = 0.025);

    MergeTreeIndexGranuleSurfFilter(const std::vector<HashSet<UInt64>> & column_hashes, double false_positive_probability = 0.025);

    MergeTreeIndexGranuleSurfFilter(const std::vector<std::set<std::string>> & column_keys, double false_positive_probability = 0.025);

    bool empty() const override;

    size_t memoryUsageBytes() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<SurfFilterPtr> & getFilters() const { return surf_filters; }
    void setFilters(const std::vector<SurfFilterPtr> & filters) { surf_filters = filters; }
    void setTotalRows(size_t rows) { total_rows = rows; }

private:
    size_t total_rows = 0;
    std::vector<SurfFilterPtr> surf_filters;

    void fillingSurfFilter(SurfFilterPtr & bf, const HashSet<UInt64> & hashes, double false_positive_probability) const;
    void fillingSurfFilterWithKeys(SurfFilterPtr & bf, const std::set<std::string> & keys, double false_positive_probability) const;
};

class MergeTreeIndexConditionSurfFilter final : public IMergeTreeIndexCondition, WithContext
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

    MergeTreeIndexConditionSurfFilter(const ActionsDAG::Node * predicate, ContextPtr context_, const Block & header_);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override
    {
        if (const auto & bf_granule = typeid_cast<const MergeTreeIndexGranuleSurfFilter *>(granule.get()))
            return mayBeTrueOnGranule(bf_granule);

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requires surf filter index granule.");
    }

private:
    const Block & header;
    std::vector<RPNElement> rpn;

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleSurfFilter * granule) const;

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

class MergeTreeIndexAggregatorSurfFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorSurfFilter(const Names & columns_name_, double false_positive_probability = 0.025);

    bool empty() const override;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    const Names index_columns_name;

    // SuRF filters for incremental insertion
    std::vector<SurfFilterPtr> surf_filters;
    // Accumulate keys for sorting before insertion
    std::vector<std::vector<std::string>> accumulated_keys;
    size_t total_rows = 0;
    double false_positive_probability;
};


class MergeTreeIndexSurfFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSurfFilter(const IndexDescription & index_, double false_positive_probability_ = 0.025);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    double getFalsePositiveProbability() const { return false_positive_probability; }

private:
    double false_positive_probability;
};

}
