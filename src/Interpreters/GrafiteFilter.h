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
    using UnderType = UInt64;
    using Container = std::vector<UnderType>;

    explicit GrafiteFilter(const GrafiteFilterParameters & params);

    /// Constructor for building Grafite from a sorted list of keys
    /// Keys must be provided in sorted order
    GrafiteFilter(const std::vector<std::string> & keys, const GrafiteFilterParameters & params);

    // /// Destructor - properly clean up Grafite memory
    // ~GrafiteFilter();

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }

    /// Point lookup - returns true if key might exist (may have false positives)
    bool lookupKey(const std::string & key) const;

    /// Range lookup - returns true if any key in range might exist
    bool lookupRange(const std::string & left_key, bool left_inclusive, const std::string & right_key, bool right_inclusive) const;

    /// Get memory usage in bytes
    size_t memoryUsageBytes() const;

    void buildFromStream(ReadBuffer & istr);

    std::shared_ptr<grafite::filter<grafite::ef_sux_vector, 2u>> readGrafite() { return std::move(grafite_); }

private:
    GrafiteFilterParameters params_;

    std::shared_ptr<grafite::filter<grafite::ef_sux_vector, 2u>> grafite_;

    // Helper methods
    void buildFromKeys(const std::vector<std::string> & keys);

    Container filter;

public:
    static ColumnPtr getPrimitiveColumn(const ColumnPtr & column);
    static DataTypePtr getPrimitiveType(const DataTypePtr & data_type);
};

typedef std::shared_ptr<GrafiteFilter> GrafiteFilterPtr;

}
