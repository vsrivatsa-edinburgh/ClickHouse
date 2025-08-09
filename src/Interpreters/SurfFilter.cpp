#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/VarInt.h>
#include <Interpreters/SurfFilter.h>

#include <algorithm>
#include <string>
#include <vector>
#include <surf.hpp>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

static LoggerPtr surf_logger = getLogger("SurfFilter");

SurfFilterParameters::SurfFilterParameters(
    bool include_dense_, UInt32 sparse_dense_ratio_, SurfSuffixType suffix_type_, UInt32 hash_suffix_len_, UInt32 real_suffix_len_)
    : include_dense(include_dense_)
    , sparse_dense_ratio(sparse_dense_ratio_)
    , suffix_type(suffix_type_)
    , hash_suffix_len(hash_suffix_len_)
    , real_suffix_len(real_suffix_len_)
{
    if (sparse_dense_ratio == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The sparse dense ratio cannot be zero");
    if (hash_suffix_len > 64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hash suffix length cannot be more than 64 bits");
    if (real_suffix_len > 256)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Real suffix length cannot be more than 256 bytes");
}


SurfFilter::SurfFilter(const SurfFilterParameters & params)
    : params_(params)
    , surf_(nullptr)
    , incremental_keys_()
    , incremental_mode_(false)
    , finalized_(false)
{
    // Initialize empty SuRF - will be created when needed
}

SurfFilter::SurfFilter(const std::vector<std::string> & keys, const SurfFilterParameters & params)
    : params_(params)
    , surf_(nullptr)
    , incremental_keys_()
    , incremental_mode_(false)
    , finalized_(false)
{
    buildFromKeys(keys);
}

void SurfFilter::initializeForIncrementalInsertion(const SurfFilterParameters & params)
{
    params_ = params;
    surf_.reset(); // Reset any existing SuRF
    incremental_keys_.clear();

    // Create new SuRF in incremental mode
    surf_ = std::make_unique<surf::SuRF>(
        params_.include_dense,
        params_.sparse_dense_ratio,
        static_cast<surf::SuffixType>(params_.suffix_type),
        params_.hash_suffix_len,
        params_.real_suffix_len);

    incremental_mode_ = true;
    finalized_ = false;
}

bool SurfFilter::insert(const std::string & key)
{
    if (!incremental_mode_ || !surf_)
    {
        return false;
    }

    // Use the SuRF library's direct insert method
    bool result = surf_->insert(key);
    return result;
}

void SurfFilter::finalize()
{
    if (!incremental_mode_ || !surf_)
    {
        return;
    }

    // Call SuRF's finalize to optimize the LOUDS-dense structure
    surf_->finalize();

    incremental_mode_ = false;
    finalized_ = true;
}

bool SurfFilter::lookupKey(const std::string & key) const
{
    if (!finalized_ || !surf_)
    {
        return false; // Not ready for lookups
    }

    bool result = surf_->lookupKey(key);
    return result;
}

// Range capabilities commented out for now
/*
bool SurfFilter::lookupRange(const std::string& left_key, bool left_inclusive, 
                            const std::string& right_key, bool right_inclusive) const
{
    // TODO: Implement range lookup when ready
    return false;
}

UInt64 SurfFilter::approxCount(const std::string& left_key, const std::string& right_key) const
{
    // TODO: Implement approximate range counting when ready
    return 0;
}
*/

void SurfFilter::clear()
{
    surf_.reset();
    incremental_keys_.clear();
    incremental_mode_ = false;
    finalized_ = false;
}

bool SurfFilter::isEmpty() const
{
    if (!surf_)
    {
        return true; // No SuRF structure created yet
    }

    if (incremental_mode_)
    {
        // In incremental mode, the structure is deemed empty until finalized
        return true;
    }

    if (finalized_)
    {
        // Check if the finalized SuRF has any keys
        return !surf_->hasKeys();
    }

    return true; // Neither incremental nor finalized, so empty
}

size_t SurfFilter::memoryUsageBytes() const
{
    if (surf_)
        return surf_->getMemoryUsage();
    return sizeof(SurfFilter) + incremental_keys_.size() * sizeof(std::string);
}

UInt32 SurfFilter::getHeight() const
{
    if (surf_)
        return surf_->getHeight();
    return 0;
}

void SurfFilter::serialize(WriteBuffer & ostr) const
{
    writeVarUInt(finalized_ ? 1 : 0, ostr);
    if (finalized_ && surf_)
    {
        auto serialized_size = surf_->serializedSize();
        writeVarUInt(serialized_size, ostr);

        char * serialized_data = surf_->serialize();
        ostr.write(serialized_data, serialized_size);
        delete[] serialized_data;
    }
    else
    {
        writeVarUInt(0, ostr); // No data to serialize
    }
}

void SurfFilter::deserialize(ReadBuffer & istr)
{
    UInt64 is_finalized;
    readVarUInt(is_finalized, istr);
    finalized_ = (is_finalized != 0);

    UInt64 serialized_size;
    readVarUInt(serialized_size, istr);

    if (serialized_size > 0)
    {
        std::vector<char> buffer(serialized_size);
        bool read_success = istr.read(buffer.data(), serialized_size);
        if (read_success)
        {
            surf_.reset(surf::SuRF::deSerialize(buffer.data()));
        }
    }
}

void SurfFilter::destroy()
{
    if (surf_)
    {
        surf_->destroy();
        surf_.reset();
    }
    clear();
}

void SurfFilter::buildFromKeys(const std::vector<std::string> & keys)
{
    if (keys.empty())
    {
        finalized_ = false;
        return;
    }

    // Create SuRF with the specified parameters
    surf_ = std::make_unique<surf::SuRF>(
        keys,
        params_.include_dense,
        params_.sparse_dense_ratio,
        static_cast<surf::SuffixType>(params_.suffix_type),
        params_.hash_suffix_len,
        params_.real_suffix_len);

    finalized_ = true;
    incremental_mode_ = false;
}

DataTypePtr SurfFilter::getPrimitiveType(const DataTypePtr & data_type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        if (!typeid_cast<const DataTypeArray *>(array_type->getNestedType().get()))
            return getPrimitiveType(array_type->getNestedType());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of surf filter index.", data_type->getName());
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
        return getPrimitiveType(nullable_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_type.get()))
        return getPrimitiveType(low_cardinality_type->getDictionaryType());

    return data_type;
}

ColumnPtr SurfFilter::getPrimitiveColumn(const ColumnPtr & column)
{
    if (const auto * array_col = typeid_cast<const ColumnArray *>(column.get()))
        return getPrimitiveColumn(array_col->getDataPtr());

    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(column.get()))
        return getPrimitiveColumn(nullable_col->getNestedColumnPtr());

    if (const auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return getPrimitiveColumn(low_cardinality_col->convertToFullColumnIfLowCardinality());

    return column;
}

void SurfFilter::add(const char * data, size_t len)
{
    if (data && len > 0)
    {
        std::string token(data, len);
        if (!incremental_mode_)
        {
            // Initialize for incremental insertion if not already done
            initializeForIncrementalInsertion(params_);
        }

        // Add the token as a key
        insert(token);
    }
}

}
