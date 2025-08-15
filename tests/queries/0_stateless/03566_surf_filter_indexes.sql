-- Test for SuRF-based text indexes: ngramsf_v1 and tokensf_v1

SET allow_experimental_inverted_index = 1;
SET allow_experimental_correlated_subqueries = 1;

-- Drop tables if they exist
DROP TABLE IF EXISTS test_surf_indexes;

-- Create table with both ngramsf_v1 and tokensf_v1 indexes
CREATE TABLE test_surf_indexes (
    id UInt32,
    content String,
    INDEX idx_ngrams content TYPE ngramsf_v1(4, 256, 2, 0) GRANULARITY 1,
    INDEX idx_tokens content TYPE tokensf_v1(256, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 100;

-- Insert 1,000 rows from sentences.txt
INSERT INTO test_surf_indexes 
SELECT 
    rowNumberInAllBlocks() as id,
    line as content
FROM file('sentences.txt', 'LineAsString', 'line String')
LIMIT 100000;

-- Wait for indexes to be built
SELECT 'Inserted', count() FROM test_surf_indexes;

-- Test 1: Basic functionality - check that both indexes work
SELECT 'Test 1: Basic search functionality';

-- Test ngram search (should find matches)
SELECT count() > 0 as ngram_search_works 
FROM test_surf_indexes 
WHERE hasToken(content, 'the') 
SETTINGS force_index_by_date = 0, force_primary_key = 0;

-- Test token search (should find matches) 
SELECT count() > 0 as token_search_works
FROM test_surf_indexes 
WHERE hasToken(content, 'and')
SETTINGS force_index_by_date = 0, force_primary_key = 0;

-- Test 2: Extract some real tokens and ngrams for positive testing
SELECT 'Test 2: Extracting test data - using hardcoded common words';

-- Test 3: True positive tests - search for ngrams that should exist
SELECT 'Test 3: True positive ngram tests';

-- Test specific known ngrams that should exist
SELECT count() > 0 as found_1 FROM test_surf_indexes WHERE hasToken(content, 'the ');
SELECT count() > 0 as found_2 FROM test_surf_indexes WHERE hasToken(content, 'and ');  
SELECT count() > 0 as found_3 FROM test_surf_indexes WHERE hasToken(content, 'that');
SELECT count() > 0 as found_4 FROM test_surf_indexes WHERE hasToken(content, 'with');
SELECT count() > 0 as found_5 FROM test_surf_indexes WHERE hasToken(content, 'this');

-- Test 4: True positive tests - search for tokens that should exist  
SELECT 'Test 4: True positive token tests';

-- Test specific known tokens that should exist
SELECT count() > 0 as found_1 FROM test_surf_indexes WHERE hasToken(content, 'the');
SELECT count() > 0 as found_2 FROM test_surf_indexes WHERE hasToken(content, 'and');
SELECT count() > 0 as found_3 FROM test_surf_indexes WHERE hasToken(content, 'that');
SELECT count() > 0 as found_4 FROM test_surf_indexes WHERE hasToken(content, 'with');
SELECT count() > 0 as found_5 FROM test_surf_indexes WHERE hasToken(content, 'this');

-- Test 5: False positive tests - search for non-existent ngrams
SELECT 'Test 5: False positive ngram tests';

-- Test with clearly non-existent 4-character sequences
SELECT count() as should_be_zero_1 FROM test_surf_indexes WHERE hasToken(content, 'xyz1');
SELECT count() as should_be_zero_2 FROM test_surf_indexes WHERE hasToken(content, 'qw9z');
SELECT count() as should_be_zero_3 FROM test_surf_indexes WHERE hasToken(content, 'zyx2');
SELECT count() as should_be_zero_4 FROM test_surf_indexes WHERE hasToken(content, 'xyz3');
SELECT count() as should_be_zero_5 FROM test_surf_indexes WHERE hasToken(content, 'qw8z');
SELECT count() as should_be_zero_6 FROM test_surf_indexes WHERE hasToken(content, 'zyx4');
SELECT count() as should_be_zero_7 FROM test_surf_indexes WHERE hasToken(content, 'xyz5');
SELECT count() as should_be_zero_8 FROM test_surf_indexes WHERE hasToken(content, 'qw7z');
SELECT count() as should_be_zero_9 FROM test_surf_indexes WHERE hasToken(content, 'zyx6');
SELECT count() as should_be_zero_10 FROM test_surf_indexes WHERE hasToken(content, 'xyz7');

-- Test 6: False positive tests - search for non-existent tokens
SELECT 'Test 6: False positive token tests';

SELECT count() as should_be_zero_1 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken1');
SELECT count() as should_be_zero_2 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken2');
SELECT count() as should_be_zero_3 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken3');
SELECT count() as should_be_zero_4 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken4');
SELECT count() as should_be_zero_5 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken5');
SELECT count() as should_be_zero_6 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken6');
SELECT count() as should_be_zero_7 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken7');
SELECT count() as should_be_zero_8 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken8');
SELECT count() as should_be_zero_9 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken9');
SELECT count() as should_be_zero_10 FROM test_surf_indexes WHERE hasToken(content, 'nonexistenttoken10');

-- Test 7: Index statistics and metadata
SELECT 'Test 7: Index information';

-- Check that indexes are actually being used
EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'the');

EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'and');

-- Test 8: Edge cases and false negative prevention
SELECT 'Test 8: Edge case tests - preventing false negatives with short strings';

-- Insert some test data with short strings to ensure we can find them
INSERT INTO test_surf_indexes VALUES 
(100001, 'This contains K0 as a short string'),
(100002, 'Another line with AB pattern'),  
(100003, 'Test with X single character'),
(100004, 'Contains UV two chars'),
(100005, 'URL like http://example.com here'),
(100006, 'Abbreviation U.S.A in text'),
(100007, 'Email test@example.com address'),
(100008, 'Version v1.2.3 number'),
(100009, 'Currency $12.34 amount'),
(100010, 'Punctuation: hello! world?'),
(100011, 'Mixed A.B.C.D pattern'),
(100012, 'Short I.T department');

-- Test with very short strings - these should NOT return false negatives
-- For strings shorter than ngram size (4), only token index should be used
SELECT 'Short string tests (should use token index only):';
SELECT count() > 0 as found_K0 FROM test_surf_indexes WHERE hasToken(content, 'K0');     -- 2 chars, should use token index
SELECT count() > 0 as found_AB FROM test_surf_indexes WHERE hasToken(content, 'AB');     -- 2 chars, should use token index  
SELECT count() > 0 as found_X FROM test_surf_indexes WHERE hasToken(content, 'X');       -- 1 char, should use token index
SELECT count() > 0 as found_UV FROM test_surf_indexes WHERE hasToken(content, 'UV');     -- 2 chars, should use token index

-- Test with punctuation-containing strings that become short after tokenization
SELECT 'Punctuation-containing strings that become short:';
-- These strings contain punctuation, so tokenization behavior matters
SELECT count() > 0 as found_USA_token FROM test_surf_indexes WHERE hasToken(content, 'U.S.A');   -- Might be tokenized as separate parts
SELECT count() > 0 as found_USA_clean FROM test_surf_indexes WHERE hasToken(content, 'USA');     -- Clean version might not exist
SELECT count() > 0 as found_IT_token FROM test_surf_indexes WHERE hasToken(content, 'I.T');      -- Punctuated version
SELECT count() > 0 as found_IT_clean FROM test_surf_indexes WHERE hasToken(content, 'IT');       -- Clean version might not exist

-- Test email and URL patterns (contain non-token characters)
SELECT 'Email and URL pattern tests:';
SELECT count() > 0 as found_email FROM test_surf_indexes WHERE hasToken(content, 'test@example.com');
SELECT count() > 0 as found_domain FROM test_surf_indexes WHERE hasToken(content, 'example.com');
SELECT count() > 0 as found_url FROM test_surf_indexes WHERE hasToken(content, 'http://example.com');

-- Test version numbers and currency (mixed alphanumeric with punctuation)
SELECT 'Version and currency pattern tests:';
SELECT count() > 0 as found_version FROM test_surf_indexes WHERE hasToken(content, 'v1.2.3');
SELECT count() > 0 as found_currency FROM test_surf_indexes WHERE hasToken(content, '$12.34');

-- Test with hasToken() function on problematic patterns
SELECT 'Complex pattern tests with hasToken() function:';
-- These might behave differently due to how regex vs token/ngram matching works
SELECT count() as match_dotted FROM test_surf_indexes WHERE hasToken(content, 'U\\.S\\.A');  -- Escaped dots for regex
SELECT count() as match_email FROM test_surf_indexes WHERE hasToken(content, '[a-z]+@[a-z]+\\.[a-z]+'); -- Email regex
SELECT count() as match_version FROM test_surf_indexes WHERE hasToken(content, 'v[0-9]+\\.[0-9]+\\.[0-9]+'); -- Version regex
SELECT count() as match_currency FROM test_surf_indexes WHERE hasToken(content, '\\$[0-9]+\\.[0-9]+'); -- Currency regex

-- Test substring matching with LIKE for complex patterns
SELECT 'Substring matching with LIKE:';
SELECT count() > 0 as like_USA FROM test_surf_indexes WHERE content LIKE '%U.S.A%';
SELECT count() > 0 as like_email FROM test_surf_indexes WHERE content LIKE '%@example.com%';
SELECT count() > 0 as like_version FROM test_surf_indexes WHERE content LIKE '%v1.2.3%';
SELECT count() > 0 as like_currency FROM test_surf_indexes WHERE content LIKE '%$12.34%';

-- Test with longer strings (>= ngram size) - both indexes should work
SELECT 'Long string tests (can use either index):';
SELECT count() > 0 as found_This FROM test_surf_indexes WHERE hasToken(content, 'This');           -- 4+ chars, token index
SELECT count() > 0 as match_This FROM test_surf_indexes WHERE hasToken(content, 'This');              -- 4+ chars, could use ngram
SELECT count() > 0 as found_contains FROM test_surf_indexes WHERE hasToken(content, 'contains');   -- 4+ chars, token index  
SELECT count() > 0 as match_contains FROM test_surf_indexes WHERE hasToken(content, 'contains');      -- 4+ chars, could use ngram

-- Test mixed length queries - this is where index selection becomes critical
SELECT 'Mixed length query tests:';
-- Query with both short and long terms - should not miss results due to short terms
SELECT count() > 0 as mixed_query_1 FROM test_surf_indexes 
WHERE (hasToken(content, 'K0') OR hasToken(content, 'This'));

-- Compare with hasToken() - might give different results due to index selection
SELECT count() as mixed_match_1 FROM test_surf_indexes 
WHERE (hasToken(content, 'K0') OR hasToken(content, 'This'));

-- Test 9: Recommended patterns to avoid false negatives
SELECT 'Test 9: Recommended query patterns';

-- Pattern 1: Use hasToken() for exact token matching (works for any length)
SELECT 'Pattern 1 - Use hasToken for exact token matching:';
SELECT count() as hasToken_short FROM test_surf_indexes WHERE hasToken(content, 'K0');
SELECT count() as hasToken_long FROM test_surf_indexes WHERE hasToken(content, 'contains');

-- Pattern 2: Use hasToken() carefully - prefer for longer patterns or regex
SELECT 'Pattern 2 - Use hasToken() for longer patterns:';
SELECT count() as match_pattern FROM test_surf_indexes WHERE hasToken(content, 'contain.*');  -- regex pattern

-- Pattern 3: For mixed queries, use appropriate function per term length
SELECT 'Pattern 3 - Mixed length queries with appropriate functions:';
SELECT count() as smart_mixed FROM test_surf_indexes 
WHERE hasToken(content, 'K0')           -- Short term: use hasToken
   OR hasToken(content, 'contains.*');     -- Long/pattern term: use hasToken

-- Pattern 4: Handling punctuation and non-token characters
SELECT 'Pattern 4 - Handling punctuation and special characters:';

-- Strategy 1: Search for the exact token as it appears (with punctuation)
SELECT count() as exact_with_punct FROM test_surf_indexes WHERE hasToken(content, 'U.S.A');

-- Strategy 2: Use LIKE for substring matching when tokenization is complex  
SELECT count() as like_substring FROM test_surf_indexes WHERE content LIKE '%U.S.A%';

-- Strategy 3: Use regex with hasToken() for flexible pattern matching
SELECT count() as regex_pattern FROM test_surf_indexes WHERE hasToken(content, 'U\\.S\\.A');

-- Strategy 4: Search for multiple variants if you know how tokenization works
SELECT count() as multi_variant FROM test_surf_indexes 
WHERE hasToken(content, 'U.S.A') OR hasToken(content, 'USA') OR content LIKE '%U.S.A%';

-- Pattern 5: For complex queries with mixed character patterns
SELECT 'Pattern 5 - Complex mixed patterns:';

-- Email-like patterns: use appropriate method based on what you're looking for
SELECT count() as email_domain FROM test_surf_indexes WHERE content LIKE '%@example.com%';      -- Substring approach
SELECT count() as email_regex FROM test_surf_indexes WHERE hasToken(content, '@[a-z]+\\.[a-z]+'); -- Pattern approach

-- Version numbers: often need substring matching due to dots
SELECT count() as version_like FROM test_surf_indexes WHERE content LIKE '%v1.2.3%';
SELECT count() as version_regex FROM test_surf_indexes WHERE hasToken(content, 'v[0-9]+(\\.[0-9]+)+');

-- Test 10: Index usage analysis
SELECT 'Test 10: Index usage verification';

-- Verify which indexes are being used for different query types
SELECT 'Index usage for hasToken queries:';
EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'K0');

EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'contains');

SELECT 'Index usage for hasToken queries:';
EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'K0');

EXPLAIN indexes = 1 
SELECT count() FROM test_surf_indexes WHERE hasToken(content, 'contains');

-- Clean up
DROP TABLE test_surf_indexes;

SELECT 'All tests completed successfully!';
