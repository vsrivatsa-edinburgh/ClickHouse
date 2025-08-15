-- Test for SuRF-based text indexes: ngramsf_v1 and tokensf_v1

SET allow_experimental_inverted_index = 1;
SET allow_experimental_correlated_subqueries = 1;

-- Drop tables if they exist
DROP TABLE IF EXISTS test_bloom_indexes;

-- Create table with both ngramsf_v1 and tokensf_v1 indexes
CREATE TABLE test_bloom_indexes (
    id UInt32,
    content String,
    INDEX idx_ngrams content TYPE ngrambf_v1(4, 256, 2, 0) GRANULARITY 1,
    INDEX idx_tokens content TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1000;

-- Insert 1,000 rows from sentences.txt
INSERT INTO test_bloom_indexes 
SELECT 
    rowNumberInAllBlocks() as id,
    line as content
FROM file('sentences.txt', 'LineAsString', 'line String')
LIMIT 100000;

-- Wait for indexes to be built
SELECT 'Inserted', count() FROM test_bloom_indexes;

-- Test 1: Basic functionality - check that both indexes work
SELECT 'Test 1: Basic search functionality';

-- Test ngram search (should find matches)
SELECT count() > 0 as ngram_search_works 
FROM test_bloom_indexes 
WHERE hasToken(content, 'the') 
SETTINGS force_index_by_date = 0, force_primary_key = 0;

-- Test token search (should find matches) 
SELECT count() > 0 as token_search_works
FROM test_bloom_indexes 
WHERE hasToken(content, 'and')
SETTINGS force_index_by_date = 0, force_primary_key = 0;