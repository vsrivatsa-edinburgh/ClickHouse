#pragma once

#include <Common/HashTable/Hash.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SurfFilter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

struct SurfFilterKey
{
    template <typename FieldGetType, typename FieldType>
    static std::string getNumberTypeKey(const Field & field)
    {
        if (field.isNull()) 
            return std::string(8, '\0'); // 8 null bytes for null values
        
        FieldType value = FieldType(field.safeGet<FieldGetType>());
        // Convert to network byte order for consistent string comparison
        if constexpr (sizeof(FieldType) == 1) {
            return std::string(1, static_cast<char>(value));
        } else if constexpr (sizeof(FieldType) == 2) {
            UInt16 net_value = htobe16(static_cast<UInt16>(value));
            return std::string(reinterpret_cast<const char*>(&net_value), sizeof(net_value));
        } else if constexpr (sizeof(FieldType) == 4) {
            UInt32 net_value = htobe32(static_cast<UInt32>(value));
            return std::string(reinterpret_cast<const char*>(&net_value), sizeof(net_value));
        } else if constexpr (sizeof(FieldType) == 8) {
            UInt64 net_value = htobe64(static_cast<UInt64>(value));
            return std::string(reinterpret_cast<const char*>(&net_value), sizeof(net_value));
        } else {
            // For larger types like UInt128, UInt256, just convert to string representation
            return toString(value);
        }
    }

    static std::string getStringTypeKey(const Field & field)
    {
        if (!field.isNull())
        {
            return field.safeGet<String>();
        }
        return ""; // Empty string for null
    }

    static std::string getFixedStringTypeKey(const Field & field, const IDataType * type)
    {
        if (!field.isNull())
        {
            return field.safeGet<String>();
        }

        const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(type);
        return std::string(fixed_string_type->getN(), '\0');
    }

    static ColumnPtr keyFromField(const IDataType * data_type, const Field & field)
    {
        const auto & build_key_column = [&](const std::string & key) -> ColumnPtr
        {
            return ColumnConst::create(ColumnString::create(std::vector<std::string>{key}), 1);
        };

        WhichDataType which(data_type);

        if (which.isUInt8())
            return build_key_column(getNumberTypeKey<UInt64, UInt8>(field));
        if (which.isUInt16())
            return build_key_column(getNumberTypeKey<UInt64, UInt16>(field));
        if (which.isUInt32())
            return build_key_column(getNumberTypeKey<UInt64, UInt32>(field));
        if (which.isUInt64())
            return build_key_column(getNumberTypeKey<UInt64, UInt64>(field));
        if (which.isUInt128())
            return build_key_column(getNumberTypeKey<UInt128, UInt128>(field));
        if (which.isUInt256())
            return build_key_column(getNumberTypeKey<UInt256, UInt256>(field));
        if (which.isInt8())
            return build_key_column(getNumberTypeKey<Int64, Int8>(field));
        if (which.isInt16())
            return build_key_column(getNumberTypeKey<Int64, Int16>(field));
        if (which.isInt32())
            return build_key_column(getNumberTypeKey<Int64, Int32>(field));
        if (which.isInt64())
            return build_key_column(getNumberTypeKey<Int64, Int64>(field));
        if (which.isInt128())
            return build_key_column(getNumberTypeKey<Int128, Int128>(field));
        if (which.isInt256())
            return build_key_column(getNumberTypeKey<Int256, Int256>(field));
        if (which.isEnum8())
            return build_key_column(getNumberTypeKey<Int64, Int8>(field));
        if (which.isEnum16())
            return build_key_column(getNumberTypeKey<Int64, Int16>(field));
        if (which.isDate())
            return build_key_column(getNumberTypeKey<UInt64, UInt16>(field));
        if (which.isDate32())
            return build_key_column(getNumberTypeKey<UInt64, Int32>(field));
        if (which.isDateTime())
            return build_key_column(getNumberTypeKey<UInt64, UInt32>(field));
        if (which.isDateTime64())
            return build_key_column(getNumberTypeKey<DateTime64, DateTime64>(field));
        if (which.isFloat32())
            return build_key_column(getNumberTypeKey<Float64, Float64>(field));
        if (which.isFloat64())
            return build_key_column(getNumberTypeKey<Float64, Float64>(field));
        if (which.isUUID())
            return build_key_column(getNumberTypeKey<UUID, UUID>(field));
        if (which.isIPv4())
            return build_key_column(getNumberTypeKey<IPv4, IPv4>(field));
        if (which.isIPv6())
            return build_key_column(getNumberTypeKey<IPv6, IPv6>(field));
        if (which.isString())
            return build_key_column(getStringTypeKey(field));
        if (which.isFixedString())
            return build_key_column(getFixedStringTypeKey(field, data_type));

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of surf filter index.", data_type->getName());
    }

    static ColumnPtr keyFromColumn(const DataTypePtr & data_type, const ColumnPtr & column, size_t pos, size_t limit)
    {
        WhichDataType which(data_type);
        if (which.isArray())
        {
            const auto * array_col = typeid_cast<const ColumnArray *>(column.get());

            if (checkAndGetColumn<ColumnNullable>(&array_col->getData()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of surf filter index.", data_type->getName());

            const auto & offsets = array_col->getOffsets();
            limit = offsets[pos + limit - 1] - offsets[pos - 1];    /// PaddedPODArray allows access on index -1.
            pos = offsets[pos - 1];

            if (limit == 0)
            {
                auto key_column = ColumnString::create();
                key_column->insertDefault(); // Insert empty string for empty array
                return key_column;
            }

            const auto & array_type = assert_cast<const DataTypeArray &>(*data_type);
            return keyFromColumn(array_type.getNestedType(), array_col->getDataPtr(), pos, limit);
        }

        auto key_column = ColumnString::create();
        
        WhichDataType which_primitive(data_type);
        
        if (which_primitive.isUInt8()) {
            const auto * uint8_col = typeid_cast<const ColumnUInt8 *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                UInt8 value = uint8_col->getElement(i);
                key_column->insertData(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        } else if (which_primitive.isUInt16()) {
            const auto * uint16_col = typeid_cast<const ColumnUInt16 *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                UInt16 value = htobe16(uint16_col->getElement(i));
                key_column->insertData(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        } else if (which_primitive.isUInt32()) {
            const auto * uint32_col = typeid_cast<const ColumnUInt32 *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                UInt32 value = htobe32(uint32_col->getElement(i));
                key_column->insertData(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        } else if (which_primitive.isUInt64()) {
            const auto * uint64_col = typeid_cast<const ColumnUInt64 *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                UInt64 value = htobe64(uint64_col->getElement(i));
                key_column->insertData(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        } else if (which_primitive.isString()) {
            const auto * string_col = typeid_cast<const ColumnString *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                StringRef str_ref = string_col->getDataAt(i);
                key_column->insertData(str_ref.data, str_ref.size);
            }
        } else if (which_primitive.isFixedString()) {
            const auto * fixed_string_col = typeid_cast<const ColumnFixedString *>(column.get());
            for (size_t i = pos; i < pos + limit; ++i) {
                StringRef str_ref = fixed_string_col->getDataAt(i);
                key_column->insertData(str_ref.data, str_ref.size);
            }
        } else {
            // For other types, fall back to string conversion
            for (size_t i = pos; i < pos + limit; ++i) {
                Field field;
                column->get(i, field);
                std::string key_str = field.dump(); // Convert field to string representation
                key_column->insertData(key_str.data(), key_str.size());
            }
        }

        return key_column;
    }

    // Utility functions for best practices calculation (maintain compatibility)
    static std::pair<size_t, size_t> calculationBestPractices(Float64 max_conflict_probability)
    {
        // For compatibility with existing interface
        // SuRF doesn't use these parameters, but we maintain the interface
        static constexpr size_t default_bits_per_row = 10;
        static constexpr size_t default_hash_functions = 5;
        
        return std::make_pair(default_bits_per_row, default_hash_functions);
    }
};

}
