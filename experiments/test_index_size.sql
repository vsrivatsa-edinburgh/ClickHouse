-- SQL queries to calculate index sizes in ClickHouse

-- 1. Basic index size query for all indexes on a table
SELECT 
    table,
    name as index_name,
    type as index_type,
    formatReadableSize(data_compressed_bytes) as compressed_size,
    formatReadableSize(data_uncompressed_bytes) as uncompressed_size,
    data_compressed_bytes,
    data_uncompressed_bytes
FROM system.data_skipping_indices 
WHERE database = 'default' 
  AND table = 'test_surf_indexes'
ORDER BY data_compressed_bytes DESC;

-- 2. Detailed breakdown by parts for SuRF indexes specifically
SELECT 
    table,
    name as index_name,
    type as index_type,
    part_name,
    formatReadableSize(data_compressed_bytes) as compressed_size,
    formatReadableSize(data_uncompressed_bytes) as uncompressed_size,
    rows,
    marks
FROM system.parts_columns 
WHERE database = 'default' 
  AND table = 'test_surf_indexes'
  AND column LIKE 'skp_idx_%'  -- ClickHouse prefix for skipping indexes
ORDER BY name, part_name;

-- 3. Compare sizes between different index types
SELECT 
    name as index_name,
    type as index_type,
    formatReadableSize(SUM(data_compressed_bytes)) as total_compressed,
    formatReadableSize(SUM(data_uncompressed_bytes)) as total_uncompressed,
    SUM(data_compressed_bytes) as compressed_bytes,
    SUM(data_uncompressed_bytes) as uncompressed_bytes,
    COUNT(*) as num_parts
FROM system.data_skipping_indices 
WHERE database = 'default' 
  AND table = 'test_surf_indexes'
GROUP BY name, type
ORDER BY compressed_bytes DESC;

-- 4. Memory usage of indexes currently loaded
SELECT 
    table,
    formatReadableSize(SUM(secondary_index_bytes)) as index_memory_usage,
    SUM(secondary_index_bytes) as index_bytes
FROM system.parts 
WHERE database = 'default' 
  AND table = 'test_surf_indexes'
  AND active = 1;

-- 5. Detailed part-level analysis
SELECT 
    name as part_name,
    formatReadableSize(bytes_on_disk) as part_size,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_index_bytes) as index_bytes,
    rows,
    marks,
    secondary_index_bytes * 100.0 / bytes_on_disk as index_percentage
FROM system.parts 
WHERE database = 'default' 
  AND table = 'test_surf_indexes'
  AND active = 1
ORDER BY bytes_on_disk DESC;

-- 6. Index efficiency comparison (size per row)
SELECT 
    i.name as index_name,
    i.type as index_type,
    formatReadableSize(i.data_compressed_bytes) as size,
    p.rows,
    i.data_compressed_bytes / p.rows as bytes_per_row,
    formatReadableSize(i.data_compressed_bytes / p.rows) as size_per_row
FROM system.data_skipping_indices i
JOIN (
    SELECT 
        table,
        SUM(rows) as rows
    FROM system.parts 
    WHERE database = 'default' 
      AND table = 'test_surf_indexes'
      AND active = 1
    GROUP BY table
) p ON i.table = p.table
WHERE i.database = 'default' 
  AND i.table = 'test_surf_indexes'
ORDER BY bytes_per_row;
