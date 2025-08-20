#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

#include <memory>
#include <string>
#include <vector>
#include <grafite.hpp>

namespace DB
{

struct GrafiteFilterParameters
{
    GrafiteFilterParameters(double bits_per_key = 2.0);

    /// bits per key
    double bits_per_key;
};

class GrafiteFilter
{
public:
    explicit GrafiteFilter(const GrafiteFilterParameters & params);

    /// Constructor for building Grafite from a sorted list of keys
    /// Keys must be provided in sorted order
    GrafiteFilter(const std::vector<std::string> & keys, const GrafiteFilterParameters & params);

    /// Destructor - properly clean up Grafite memory
    ~GrafiteFilter();

    /// Copy constructor and assignment operator are deleted due to unique_ptr
    GrafiteFilter(const GrafiteFilter &) = delete;
    GrafiteFilter & operator=(const GrafiteFilter &) = delete;

    /// Move constructor and assignment operator
    GrafiteFilter(GrafiteFilter &&) = default;
    GrafiteFilter & operator=(GrafiteFilter &&) = default;

    /// Point lookup - returns true if key might exist (may have false positives)
    bool lookupKey(const std::string & key) const;

    /// Range lookup - returns true if any key in range might exist
    bool lookupRange(const std::string & left_key, bool left_inclusive, const std::string & right_key, bool right_inclusive) const;

    /// Get memory usage in bytes
    size_t memoryUsageBytes() const;

private:
    GrafiteFilterParameters params_;

    std::unique_ptr<grafite::filter<int>> grafite_;

    // Helper methods
    void buildFromKeys(const std::vector<std::string> & keys);

public:
    static ColumnPtr getPrimitiveColumn(const ColumnPtr & column);
    static DataTypePtr getPrimitiveType(const DataTypePtr & data_type);
};

typedef std::shared_ptr<GrafiteFilter> GrafiteFilterPtr;

}
