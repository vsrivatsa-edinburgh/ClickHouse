#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/GrafiteFilter.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <grafite.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

GrafiteFilterParameters::GrafiteFilterParameters(double bits_per_key_)
    : bits_per_key(bits_per_key_)
{
    if (bits_per_key_ <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bits per key must be positive");
}

GrafiteFilter::GrafiteFilter(const GrafiteFilterParameters & params)
    : params_(params)
    , grafite_(nullptr)
{
    // Initialize empty Grafite - will be created when needed
}

GrafiteFilter::GrafiteFilter(const std::vector<std::string> & keys, const GrafiteFilterParameters & params)
    : params_(params)
    , grafite_(nullptr)
{
    buildFromKeys(keys);
}

GrafiteFilter::~GrafiteFilter() = default;

bool GrafiteFilter::lookupKey(const std::string & key) const
{
    if (!grafite_)
        return false; // Not ready for lookups

    // Cast string pointer to int directly since keys are stringified integers
    int int_value = static_cast<int>(reinterpret_cast<uintptr_t>(key.c_str()));
    return grafite_->query(int_value, int_value);
}

bool GrafiteFilter::lookupRange(
    const std::string & left_key, bool left_inclusive, const std::string & right_key, bool right_inclusive) const
{
    if (!grafite_)
        return false; // Not ready for lookups

    int left_int = static_cast<int>(reinterpret_cast<uintptr_t>(left_key.c_str()));
    int right_int = static_cast<int>(reinterpret_cast<uintptr_t>(right_key.c_str()));

    if (left_inclusive && right_inclusive)
        return grafite_->query(left_int, right_int);
    else if (left_inclusive)
        return grafite_->query(left_int, right_int - 1);
    else if (right_inclusive)
        return grafite_->query(left_int + 1, right_int);

    return grafite_->query(left_int, right_int);
}

size_t GrafiteFilter::memoryUsageBytes() const
{
    if (grafite_)
        return sizeof(*grafite_);
    return sizeof(GrafiteFilter);
}

void GrafiteFilter::buildFromKeys(const std::vector<std::string> & keys)
{
    std::vector<int> int_keys;
    int_keys.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (i < 10)
            LOG_TRACE(getLogger("GrafiteFilter"), "Inserting key[{}]: '{}'", i, keys[i]);
        // Cast string pointer to int directly since keys are stringified integers
        int_keys.push_back(static_cast<int>(reinterpret_cast<uintptr_t>(keys[i].c_str())));
    }

    if (keys.size() > 10)
        LOG_TRACE(getLogger("GrafiteFilter"), "... and {} more keys", keys.size() - 10);

    // Sort the keys before inserting for better filter performance
    std::sort(int_keys.begin(), int_keys.end());

    // Create Grafite with the casted keys using bits per key from params
    grafite_ = std::make_unique<grafite::filter<int>>(int_keys.begin(), int_keys.end(), params_.bits_per_key);
}

DataTypePtr GrafiteFilter::getPrimitiveType(const DataTypePtr & data_type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        if (!typeid_cast<const DataTypeArray *>(array_type->getNestedType().get()))
            return getPrimitiveType(array_type->getNestedType());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of grafite filter index.", data_type->getName());
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
        return getPrimitiveType(nullable_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_type.get()))
        return getPrimitiveType(low_cardinality_type->getDictionaryType());

    return data_type;
}

ColumnPtr GrafiteFilter::getPrimitiveColumn(const ColumnPtr & column)
{
    if (const auto * array_col = typeid_cast<const ColumnArray *>(column.get()))
        return getPrimitiveColumn(array_col->getDataPtr());

    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(column.get()))
        return getPrimitiveColumn(nullable_col->getNestedColumnPtr());

    if (const auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return getPrimitiveColumn(low_cardinality_col->convertToFullColumnIfLowCardinality());

    return column;
}

}
