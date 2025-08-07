#include <Interpreters/SurfFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/VarInt.h>

#include <surf.hpp>
#include <string>
#include <vector>
#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

SurfFilterParameters::SurfFilterParameters(
    bool include_dense_,
    UInt32 sparse_dense_ratio_,
    SurfSuffixType suffix_type_,
    UInt32 hash_suffix_len_,
    UInt32 real_suffix_len_)
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
    // Initialize empty SuRF
}

SurfFilter::SurfFilter(const std::vector<std::string>& keys, const SurfFilterParameters & params)
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
    incremental_mode_ = true;
    finalized_ = false;
}

bool SurfFilter::insert(const std::string& key)
{
    if (!incremental_mode_)
        return false;
    
    // Check if the key maintains sorted order
    if (!incremental_keys_.empty() && key < incremental_keys_.back())
        return false; // Violates sort order
    
    incremental_keys_.push_back(key);
    return true;
}

void SurfFilter::finalize()
{
    if (!incremental_mode_ || incremental_keys_.empty())
        return;
        
    // Build the final SuRF from accumulated keys
    buildFromKeys(incremental_keys_);
    incremental_keys_.clear();
    incremental_mode_ = false;
    finalized_ = true;
}

bool SurfFilter::lookupKey(const std::string& key) const
{
    if (!finalized_ || !surf_)
        return false; // Not ready for lookups
        
    return surf_->lookupKey(key);
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
    return !finalized_ && !incremental_mode_;
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
    if (finalized_ && surf_) {
        auto serialized_size = surf_->serializedSize();
        writeVarUInt(serialized_size, ostr);
        
        char* serialized_data = surf_->serialize();
        ostr.write(serialized_data, serialized_size);
        delete[] serialized_data;
    } else {
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
    
    if (serialized_size > 0) {
        std::vector<char> buffer(serialized_size);
        bool read_success = istr.read(buffer.data(), serialized_size);
        if (read_success) {
            surf_.reset(surf::SuRF::deSerialize(buffer.data()));
        }
    }
}

void SurfFilter::destroy()
{
    if (surf_) {
        surf_->destroy();
        surf_.reset();
    }
    clear();
}

void SurfFilter::buildFromKeys(const std::vector<std::string>& keys)
{
    if (keys.empty()) {
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
        params_.real_suffix_len
    );
    
    finalized_ = true;
    incremental_mode_ = false;
}

void SurfFilter::createFromBuilder()
{
    // This would be used if we had a builder-based approach
    finalized_ = true;
    incremental_mode_ = false;
}

bool operator== (const SurfFilter & a, const SurfFilter & b)
{
    // Compare basic state and parameters
    if (a.finalized_ != b.finalized_ || 
        a.incremental_mode_ != b.incremental_mode_ ||
        a.params_.include_dense != b.params_.include_dense ||
        a.params_.sparse_dense_ratio != b.params_.sparse_dense_ratio ||
        a.params_.suffix_type != b.params_.suffix_type)
        return false;
    
    // If both have SuRF instances, we can't easily compare them directly
    // This is a limitation of the current implementation
    return (a.surf_ == nullptr) == (b.surf_ == nullptr);
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

void SurfFilter::add(const char* data, size_t len)
{
    if (data && len > 0) {
        std::string token(data, len);
        add(token);
    }
}

void SurfFilter::add(const std::string& token)
{
    if (!incremental_mode_) {
        // Initialize for incremental insertion if not already done
        initializeForIncrementalInsertion(params_);
    }
    
    // Add the token as a key
    insert(token);
}

}
