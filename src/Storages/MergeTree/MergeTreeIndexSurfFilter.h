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
    MergeTreeIndexGranuleSurfFilter(size_t index_columns_);

    MergeTreeIndexGranuleSurfFilter(const std::vector<HashSet<UInt64>> & column_hashes);

    MergeTreeIndexGranuleSurfFilter(const std::vector<std::set<std::string>> & column_keys);

    bool empty() const override;

    size_t memoryUsageBytes() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<SurfFilterPtr> & getFilters() const { return surf_filters; }

private:
    size_t total_rows = 0;
    std::vector<SurfFilterPtr> surf_filters;

    void fillingSurfFilter(SurfFilterPtr & bf, const HashSet<UInt64> & hashes) const;
    void fillingSurfFilterWithKeys(SurfFilterPtr & bf, const std::set<std::string> & keys) const;
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
    MergeTreeIndexAggregatorSurfFilter(const Names & columns_name_);

    bool empty() const override;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    const Names index_columns_name;

    // Store keys for SuRF construction instead of hashes
    std::vector<std::set<std::string>> column_keys;
    // Keep hashes for backward compatibility during transition
    std::vector<HashSet<UInt64>> column_hashes;
    size_t total_rows = 0;
};


class MergeTreeIndexSurfFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSurfFilter(const IndexDescription & index_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
};

}
