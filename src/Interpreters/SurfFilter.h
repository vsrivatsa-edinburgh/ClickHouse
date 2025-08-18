#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

#include <memory>
#include <string>
#include <vector>

// Include SuRF header from contrib library
#include <surf.hpp>


namespace DB
{

/**
 * SuRF (Succinct Range Filter) Implementation
 * 
 * SuRF is a fast and compact data structure that provides:
 * - Exact-match filtering (like Bloom filters) - IMPLEMENTED
 * - Range filtering (unlike Bloom filters) - PLANNED (commented out for now)
 * - Approximate range counts - PLANNED (commented out for now)
 * 
 * Current Implementation Status:
 * - Point queries: Basic structure in place (needs actual trie implementation)
 * - Range queries: Interface defined but commented out
 * - Incremental construction: Interface ready for implementation
 * 
 * Key differences from Bloom filters:
 * - Can answer range queries (e.g., keys between "apple" and "zebra") - when implemented
 * - Uses trie-based structure instead of hash-based bit array
 * - Supports incremental construction and iteration
 * - Can provide approximate counts for ranges - when implemented
 * 
 * Parameters:
 * - include_dense: Include dense layer for better performance on short keys
 * - sparse_dense_ratio: Memory/performance tradeoff (16 = balanced, lower = more memory/faster)
 * - suffix_type: Type of suffix stored to reduce false positives
 *   - kNone: No suffixes (fastest, highest false positive rate)
 *   - kHash: Hash-based suffixes (good balance)
 *   - kReal: Real string suffixes (slower, lowest false positive rate)  
 *   - kMixed: Combination of hash and real suffixes
 * - hash_suffix_len: Length of hash suffixes (typically 4-8 bits)
 * - real_suffix_len: Length of real suffixes (typically 4-8 bytes)
 */

enum SurfSuffixType
{
    kNone = 0,
    kHash = 1,
    kReal = 2,
    kMixed = 3
};

struct SurfFilterParameters
{
    SurfFilterParameters(
        bool include_dense_ = true,
        UInt32 sparse_dense_ratio_ = 16,
        SurfSuffixType suffix_type_ = kNone,
        UInt32 hash_suffix_len_ = 0,
        UInt32 real_suffix_len_ = 0);

    /// Whether to include dense layer for better performance on short keys
    bool include_dense;
    /// Ratio between sparse and dense layers (default: 16, smaller = more memory but faster)
    UInt32 sparse_dense_ratio;
    /// Type of suffix to store for reducing false positives
    SurfSuffixType suffix_type;
    /// Length of hash suffix (used with kHash or kMixed suffix types)
    UInt32 hash_suffix_len;
    /// Length of real suffix (used with kReal or kMixed suffix types)
    UInt32 real_suffix_len;
};

class SurfFilter
{
public:
    typedef UInt64 UnderType;
    typedef std::vector<UnderType> Container;

    explicit SurfFilter(const SurfFilterParameters & params);

    /// Constructor for building SuRF from a sorted list of keys
    /// Keys must be provided in sorted order
    SurfFilter(const std::vector<std::string> & keys, const SurfFilterParameters & params);

    /// Destructor - properly clean up SuRF memory
    ~SurfFilter();

    /// Copy constructor and assignment operator are deleted due to unique_ptr
    SurfFilter(const SurfFilter &) = delete;
    SurfFilter & operator=(const SurfFilter &) = delete;

    /// Move constructor and assignment operator
    SurfFilter(SurfFilter &&) = default;
    SurfFilter & operator=(SurfFilter &&) = default;

    /// Initialize empty SuRF for incremental insertion
    void initializeForIncrementalInsertion(const SurfFilterParameters & params);

    /// Insert a single key (must be in sorted order relative to previous keys)
    bool insert(const std::string & key);

    /// Finalize the SuRF after incremental insertions
    void finalize();

    /// Point lookup - returns true if key might exist (may have false positives)
    bool lookupKey(const std::string & key) const;

    /// Check if this SuRF contains all tokens from another SuRF
    /// For text indexes: returns true if all tokens in the query filter exist in this filter
    bool contains(const std::vector<std::string> & tokens) const;

    /// Range lookup - returns true if any key in range might exist
    bool lookupRange(const std::string & left_key, bool left_inclusive, const std::string & right_key, bool right_inclusive) const;

    /// Approximate count of keys in range (may undercount by at most 2)
    UInt64 approxCount(const std::string & left_key, const std::string & right_key) const;

    /// Clear all data
    void clear();

    /// Check if SuRF is empty
    bool isEmpty() const;

    /// Get memory usage in bytes
    size_t memoryUsageBytes() const;

    /// Get height of the trie structure
    UInt32 getHeight() const;

    /// Serialize SuRF to buffer
    void serialize(WriteBuffer & ostr) const;

    /// Deserialize SuRF from buffer
    void deserialize(ReadBuffer & istr);

    /// Destroy internal structures
    void destroy();

    /// Legacy compatibility method for token extraction
    /// Adds a token to the filter (converts to string and inserts)
    void add(const char * data, size_t len);


private:
    SurfFilterParameters params_;

    // Use actual SuRF implementation from contrib
    std::unique_ptr<surf::SuRF> surf_;
    bool incremental_mode_;
    bool finalized_;

    // Helper methods
    void buildFromKeys(const std::vector<std::string> & keys);

public:
    static ColumnPtr getPrimitiveColumn(const ColumnPtr & column);
    static DataTypePtr getPrimitiveType(const DataTypePtr & data_type);
};

typedef std::shared_ptr<SurfFilter> SurfFilterPtr;

}
