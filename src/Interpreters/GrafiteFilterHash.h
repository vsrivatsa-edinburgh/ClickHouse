#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/GrafiteFilter.h>
#include <fmt/format.h>
#include <Common/HashTable/Hash.h>
#include <Common/formatReadable.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}

struct GrafiteFilterHash
{
    template <typename FieldGetType, typename FieldType>
    static UInt64 getNumberTypeHash(const Field & field)
    {
        /// For negative, we should convert the type to make sure the symbol is in right place
        return field.isNull() ? intHash64(0) : DefaultHash64<FieldType>(FieldType(field.safeGet<FieldGetType>()));
    }

    static ColumnPtr hashWithField(const IDataType * data_type, const Field & field)
    {
        const auto & build_hash_column
            = [&](const UInt64 & hash) -> ColumnPtr { return ColumnConst::create(ColumnUInt64::create(1, hash), 1); };


        WhichDataType which(data_type);

        if (which.isUInt8())
            return build_hash_column(getNumberTypeHash<UInt64, UInt8>(field));
        if (which.isUInt16())
            return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        if (which.isUInt32())
            return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        if (which.isUInt64())
            return build_hash_column(getNumberTypeHash<UInt64, UInt64>(field));
        if (which.isUInt128())
            return build_hash_column(getNumberTypeHash<UInt128, UInt256>(field));
        if (which.isUInt256())
            return build_hash_column(getNumberTypeHash<UInt256, UInt256>(field));
        if (which.isInt8())
            return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        if (which.isInt16())
            return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        if (which.isInt32())
            return build_hash_column(getNumberTypeHash<Int64, Int32>(field));
        if (which.isInt64())
            return build_hash_column(getNumberTypeHash<Int64, Int64>(field));
        if (which.isInt128())
            return build_hash_column(getNumberTypeHash<Int128, Int128>(field));
        if (which.isInt256())
            return build_hash_column(getNumberTypeHash<Int256, Int256>(field));
        if (which.isEnum8())
            return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        if (which.isEnum16())
            return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        if (which.isDate())
            return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        if (which.isDate32())
            return build_hash_column(getNumberTypeHash<UInt64, Int32>(field));
        if (which.isDateTime())
            return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of grafite filter index.", data_type->getName());
    }

    // New function to create key column instead of hash column for Grafite key-based lookups
    static ColumnPtr keyWithField(const IDataType * data_type, const Field & field)
    {
        const auto & build_key_column = [&](const String & key) -> ColumnPtr
        {
            auto string_column = ColumnString::create();
            string_column->insert(key);
            return ColumnConst::create(std::move(string_column), 1);
        };

        WhichDataType which(data_type);

        // Convert field to string representation that SuRF can use
        if (which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64() || which.isUInt128() || which.isUInt256())
        {
            return build_key_column(field.isNull() ? "0" : toString(field.safeGet<UInt64>()));
        }
        if (which.isInt8() || which.isInt16() || which.isInt32() || which.isInt64() || which.isInt128() || which.isInt256())
        {
            return build_key_column(field.isNull() ? "0" : toString(field.safeGet<Int64>()));
        }
        if (which.isEnum8() || which.isEnum16() || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64())
        {
            return build_key_column(field.isNull() ? "0" : toString(field.safeGet<UInt64>()));
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of grafite filter index.", data_type->getName());
    }

    static ColumnPtr hashWithColumn(const DataTypePtr & data_type, const ColumnPtr & column, size_t pos, size_t limit)
    {
        WhichDataType which(data_type);
        if (which.isArray())
        {
            const auto * array_col = typeid_cast<const ColumnArray *>(column.get());

            if (checkAndGetColumn<ColumnNullable>(&array_col->getData()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of grafite filter index.", data_type->getName());

            const auto & offsets = array_col->getOffsets();
            limit = offsets[pos + limit - 1] - offsets[pos - 1]; /// PaddedPODArray allows access on index -1.
            pos = offsets[pos - 1];

            if (limit == 0)
            {
                auto index_column = ColumnUInt64::create(1);
                ColumnUInt64::Container & index_column_vec = index_column->getData();
                index_column_vec[0] = 0;
                return index_column;
            }
        }

        const ColumnPtr actual_col = GrafiteFilter::getPrimitiveColumn(column);
        const DataTypePtr actual_type = GrafiteFilter::getPrimitiveType(data_type);

        auto index_column = ColumnUInt64::create(limit);
        ColumnUInt64::Container & index_column_vec = index_column->getData();
        getAnyTypeHash<true>(actual_type.get(), actual_col.get(), index_column_vec, pos);
        return index_column;
    }

    template <bool is_first>
    static void getAnyTypeHash(const IDataType * data_type, const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        WhichDataType which(data_type);

        if (which.isUInt8())
            getNumberTypeHash<UInt8, is_first>(column, vec, pos);
        else if (which.isUInt16())
            getNumberTypeHash<UInt16, is_first>(column, vec, pos);
        else if (which.isUInt32())
            getNumberTypeHash<UInt32, is_first>(column, vec, pos);
        else if (which.isUInt64())
            getNumberTypeHash<UInt64, is_first>(column, vec, pos);
        else if (which.isUInt128())
            getNumberTypeHash<UInt128, is_first>(column, vec, pos);
        else if (which.isUInt256())
            getNumberTypeHash<UInt256, is_first>(column, vec, pos);
        else if (which.isInt8())
            getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isInt16())
            getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isInt32())
            getNumberTypeHash<Int32, is_first>(column, vec, pos);
        else if (which.isInt64())
            getNumberTypeHash<Int64, is_first>(column, vec, pos);
        else if (which.isInt128())
            getNumberTypeHash<Int128, is_first>(column, vec, pos);
        else if (which.isInt256())
            getNumberTypeHash<Int256, is_first>(column, vec, pos);
        else if (which.isEnum8())
            getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isEnum16())
            getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isDate())
            getNumberTypeHash<UInt16, is_first>(column, vec, pos);
        else if (which.isDate32())
            getNumberTypeHash<Int32, is_first>(column, vec, pos);
        else if (which.isDateTime())
            getNumberTypeHash<UInt32, is_first>(column, vec, pos);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of grafite filter index.", data_type->getName());
    }

    template <typename Type, bool is_first>
    static void getNumberTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        const auto * index_column = typeid_cast<const ColumnVector<Type> *>(column);

        if (unlikely(!index_column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} was passed to the grafite filter index", column->getName());

        const typename ColumnVector<Type>::Container & vec_from = index_column->getData();

        /// Because we're missing the precision of float in the Field.h
        /// to be consistent, we need to convert Float32 to Float64 processing, also see: GrafiteFilterHash::hashWithField
        if constexpr (std::is_same_v<ColumnVector<Type>, ColumnFloat32>)
        {
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 hash = DefaultHash64<Float64>(Float64(vec_from[index + pos]));

                if constexpr (is_first)
                    vec[index] = hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash));
            }
        }
        else
        {
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 hash = DefaultHash64<Type>(vec_from[index + pos]);

                if constexpr (is_first)
                    vec[index] = hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash));
            }
        }
    }

    template <bool is_first>
    static void getStringTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        if (const auto * index_column = typeid_cast<const ColumnString *>(column))
        {
            const ColumnString::Chars & data = index_column->getChars();
            const ColumnString::Offsets & offsets = index_column->getOffsets();

            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                ColumnString::Offset current_offset = offsets[index + pos - 1];
                size_t length = offsets[index + pos] - current_offset - 1 /* terminating zero */;
                UInt64 city_hash = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&data[current_offset]), length);

                if constexpr (is_first)
                    vec[index] = city_hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], city_hash));
            }
        }
        else if (const auto * fixed_string_index_column = typeid_cast<const ColumnFixedString *>(column))
        {
            size_t fixed_len = fixed_string_index_column->getN();
            const auto & data = fixed_string_index_column->getChars();

            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 city_hash = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&data[(index + pos) * fixed_len]), fixed_len);

                if constexpr (is_first)
                    vec[index] = city_hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], city_hash));
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type was passed to the grafite filter index.");
    }
};

}
