#!/usr/bin/env python3
"""
SuRF vs Bloom Filter Performance Evaluation Script - N-gram and Token Strategies

This script compares the performance of SuRF vs Bloom filters for n-gram and token strategies.
Uses sentence data from sentences.txt file with first 1 million entries.
Tests: ngramsf_v1 vs ngrambf_v1 and tokensf_v1 vs tokenbf_v1
"""

import subprocess
import random
import time
import json
import argparse
import signal
import os
import string
from typing import List, Dict, Tuple
import uuid

class ClickHouseIndexEvaluator:
    def __init__(self, clickhouse_client_path='./build/programs/clickhouse'):
        """Initialize with ClickHouse client path"""
        self.client_path = clickhouse_client_path
        self.server_path = clickhouse_client_path
        self.server_process = None
        self.inserted_sentences = []  # Store inserted sentences for query generation
        print(f"🎯 ClickHouse Index Evaluator initialized for N-gram and Token strategies")
        
    def start_clickhouse_server(self):
        """Start ClickHouse server"""
        print("🚀 Starting ClickHouse server...")
        try:
            # Start server in background
            self.server_process = subprocess.Popen(
                [self.server_path, 'server', '--config-file=./programs/server/config.xml'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )
            
            # Wait for server to start
            print("⏳ Waiting for server to initialize...")
            time.sleep(5)
            
            # Test connection
            for attempt in range(10):
                result, success = self.execute_query("SELECT 1")
                if success:
                    print("✅ ClickHouse server started successfully")
                    return True
                print(f"⏳ Waiting for ClickHouse to start (attempt {attempt + 1}/10)...")
                time.sleep(2)
            
            print("❌ Failed to start ClickHouse server")
            return False
            
        except Exception as e:
            print(f"❌ Error starting ClickHouse server: {e}")
            return False
    
    def stop_clickhouse_server(self):
        """Stop ClickHouse server"""
        if self.server_process:
            print("🛑 Stopping ClickHouse server...")
            try:
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(self.server_process.pid), signal.SIGTERM)
                
                # Wait for graceful shutdown
                try:
                    self.server_process.wait(timeout=10)
                    print("✅ Server stopped gracefully")
                except subprocess.TimeoutExpired:
                    # Force kill if needed
                    os.killpg(os.getpgid(self.server_process.pid), signal.SIGKILL)
                    print("⚡ Server force killed")
                    
            except Exception as e:
                print(f"⚠️  Error stopping server: {e}")
            
            self.server_process = None
            time.sleep(2)  # Brief delay after shutdown
    
    def restart_clickhouse_server(self):
        """Restart ClickHouse server"""
        print("🔄 Restarting ClickHouse server...")
        self.stop_clickhouse_server()
        time.sleep(3)  # Delay between stop and start
        return self.start_clickhouse_server()
        
    def execute_query(self, query: str) -> Tuple[str, bool]:
        """Execute a query using ClickHouse client"""
        try:
            cmd = [self.client_path, 'client', '--query', query]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.stdout.strip(), result.returncode == 0
        except subprocess.TimeoutExpired:
            return "", False
        except Exception as e:
            print(f"Query execution error: {e}")
            return "", False
    
    def get_filtering_marks_metric(self) -> int:
        """Get current value of FilteringMarksWithSecondaryKeysMicroseconds metric"""
        query = "SELECT value FROM system.events WHERE event = 'FilteringMarksWithSecondaryKeysMicroseconds'"
        result, success = self.execute_query(query)
        if success and result.strip():
            try:
                return int(result.strip())
            except ValueError:
                return 0
        return 0
    
    def delete_tables_if_exist(self, table_names: List[str]):
        """Delete tables if they exist"""
        for table_name in table_names:
            query = f"DROP TABLE IF EXISTS {table_name}"
            result, success = self.execute_query(query)
            if success:
                print(f"✓ Dropped table {table_name}")
            else:
                print(f"✗ Error dropping table {table_name}")

    def create_surf_ngram_table(self, table_name: str, granularity: int) -> bool:
        """Create table for SuRF n-gram testing"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            sentence String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created SuRF n-gram table {table_name}")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            return False

    def create_bloom_ngram_table(self, table_name: str, granularity: int) -> bool:
        """Create table for Bloom n-gram testing"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            sentence String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created Bloom n-gram table {table_name}")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            return False

    def create_surf_token_table(self, table_name: str, granularity: int) -> bool:
        """Create table for SuRF token testing"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            sentence String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created SuRF token table {table_name}")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            return False

    def create_bloom_token_table(self, table_name: str, granularity: int) -> bool:
        """Create table for Bloom token testing"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            sentence String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created Bloom token table {table_name}")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            return False
    
    def create_surf_ngram_index(self, table_name: str, n: int, table_nonce: str = None) -> float:
        """Create SuRF n-gram index and measure creation time"""
        print(f"🔄 Creating SuRF n-gram index on {table_name} with n={n}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_sentence sentence TYPE ngramsf_v1({n}, 512, 3, 0) GRANULARITY 1
        """
        
        print("📝 Adding SuRF n-gram index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding SuRF n-gram index definition: {result}")
            return 0.0
        
        print("✓ SuRF n-gram index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting SuRF n-gram index materialization...")
        start_time = time.time()
        
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_sentence{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
        if not success:
            print(f"✗ Error materializing SuRF n-gram index: {result}")
            return 0.0
        
        materialization_time = end_time - start_time
        print(f"✓ SuRF n-gram index materialized in {materialization_time:.3f} seconds")
        return materialization_time

    def create_bloom_ngram_index(self, table_name: str, n: int, table_nonce: str = None) -> float:
        """Create Bloom n-gram index and measure creation time"""
        print(f"🔄 Creating Bloom n-gram index on {table_name} with n={n}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_sentence sentence TYPE ngrambf_v1({n}, 512, 3, 0) GRANULARITY 1
        """
        
        print("📝 Adding Bloom n-gram index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding Bloom n-gram index definition: {result}")
            return 0.0
        
        print("✓ Bloom n-gram index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting Bloom n-gram index materialization...")
        start_time = time.time()
        
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_sentence{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
        if not success:
            print(f"✗ Error materializing Bloom n-gram index: {result}")
            return 0.0
        
        materialization_time = end_time - start_time
        print(f"✓ Bloom n-gram index materialized in {materialization_time:.3f} seconds")
        return materialization_time

    def create_surf_token_index(self, table_name: str, table_nonce: str = None) -> float:
        """Create SuRF token index and measure creation time"""
        print(f"🔄 Creating SuRF token index on {table_name}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_sentence sentence TYPE tokensf_v1(512, 3, 0) GRANULARITY 1
        """
        
        print("📝 Adding SuRF token index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding SuRF token index definition: {result}")
            return 0.0
        
        print("✓ SuRF token index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting SuRF token index materialization...")
        start_time = time.time()
        
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_sentence{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
        if not success:
            print(f"✗ Error materializing SuRF token index: {result}")
            return 0.0
        
        materialization_time = end_time - start_time
        print(f"✓ SuRF token index materialized in {materialization_time:.3f} seconds")
        return materialization_time

    def create_bloom_token_index(self, table_name: str, table_nonce: str = None) -> float:
        """Create Bloom token index and measure creation time"""
        print(f"🔄 Creating Bloom token index on {table_name}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_sentence sentence TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
        """
        
        print("📝 Adding Bloom token index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding Bloom token index definition: {result}")
            return 0.0
        
        print("✓ Bloom token index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting Bloom token index materialization...")
        start_time = time.time()
        
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_sentence{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
        if not success:
            print(f"✗ Error materializing Bloom token index: {result}")
            return 0.0
        
        materialization_time = end_time - start_time
        print(f"✓ Bloom token index materialized in {materialization_time:.3f} seconds")
        return materialization_time
        
        if not success:
            print(f"✗ Error materializing SuRF index: {result}")
            return 0.0
        
        # Calculate materialization time
        materialization_time = end_time - start_time
        
        # Also try to get more precise timing from query_log
        time.sleep(2)  # Wait for query_log to be updated
        query_log_time = self.get_index_creation_time_from_query_log(start_time, table_name, table_nonce, "MATERIALIZE INDEX")
        
        # Use query_log time if available, otherwise use our measured time
        creation_time = query_log_time if query_log_time > 0 else materialization_time
        
        print(f"✓ SuRF index materialized in {creation_time:.3f} seconds")
        return creation_time
        
    def insert_sentence_data(self, table_name: str, num_rows: int = 1000000):
        """Insert sentence data from sentences.txt file using ClickHouse File engine"""
        print(f"🔄 Inserting {num_rows} sentences into {table_name} from sentences.txt using File engine...")
        
        # Load sentences into memory for query generation (only first time)
        if not hasattr(self, 'inserted_sentences') or not self.inserted_sentences:
            try:
                with open('user_files/sentences.txt', 'r', encoding='utf-8') as f:
                    sentences = []
                    for i, line in enumerate(f):
                        if i >= num_rows:
                            break
                        sentence = line.strip()
                        if sentence:  # Skip empty lines
                            sentences.append(sentence)
                    
                    # Store sentences in memory for query generation
                    self.inserted_sentences = sentences
                    print(f"📋 Loaded {len(sentences)} sentences into memory for query generation")
                    
            except FileNotFoundError:
                print("❌ Error: sentences.txt file not found in user_files/ directory")
                return
            except Exception as e:
                print(f"❌ Error reading sentences file: {e}")
                return
        else:
            print(f"📋 Using previously loaded {len(self.inserted_sentences)} sentences for query generation")
        
        # Add delay before insertion
        print("⏳ Delay before insertion...")
        time.sleep(2)
        
        # Create INSERT query using File engine to read directly from file
        insert_query = f"""
        INSERT INTO {table_name} (sentence)
        SELECT line as sentence
        FROM file('sentences.txt', 'LineAsString')
        WHERE line != ''
        LIMIT {num_rows}
        """
        
        # Execute insert query using ClickHouse File engine
        print("📤 Inserting sentence data using ClickHouse File engine...")
        result, success = self.execute_query(insert_query)
        
        if not success:
            print(f"✗ Error inserting sentences: {result}")
            return
        
        print(f"✓ Sentence data insertion completed using File engine")
        
        # Add delay after insertion
        print("⏳ Delay after insertion...")
        time.sleep(3)
        
        # Get actual row count
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count_result, count_success = self.execute_query(count_query)
        
        if count_success:
            actual_rows = int(count_result.strip()) if count_result.strip() else 0
            print(f"✓ Inserted {actual_rows} string rows into {table_name}")
        else:
            print(f"✓ Inserted string data into {table_name}")
        
        print("💥 Crashing server after insertion to test persistence...")
        time.sleep(1)
    
    def build_ngram_granule_map(self, table_name: str, n: int = 3) -> Dict[str, int]:
        """Build a mapping of n-grams to the number of granules they appear in"""
        print(f"🔍 Building n-gram to granule mapping for {table_name} with n={n}...")
        
        # Query to get granule information for each sentence
        granule_query = f"""
        SELECT 
            sentence,
            _part_index,
            intDiv(rowNumberInAllBlocks(), 512) as granule_id
        FROM {table_name}
        ORDER BY _part_index, granule_id
        """
        
        result, success = self.execute_query(granule_query)
        if not success:
            print(f"❌ Failed to get granule mapping: {result}")
            return {}
        
        ngram_granule_map = {}
        
        # Process each sentence and track which granules contain which n-grams
        for line in result.strip().split('\n'):
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) >= 3:
                sentence = parts[0]
                part_index = int(parts[1])
                granule_id = int(parts[2])
                granule_key = f"{part_index}_{granule_id}"
                
                # Generate all n-grams from this sentence
                if len(sentence) >= n:
                    for i in range(len(sentence) - n + 1):
                        ngram = sentence[i:i + n]
                        if ngram not in ngram_granule_map:
                            ngram_granule_map[ngram] = set()
                        ngram_granule_map[ngram].add(granule_key)
        
        # Convert sets to counts
        ngram_count_map = {ngram: len(granules) for ngram, granules in ngram_granule_map.items()}
        
        print(f"✓ Built n-gram mapping: {len(ngram_count_map)} unique n-grams across granules")
        return ngram_count_map

    def build_token_granule_map(self, table_name: str) -> Dict[str, int]:
        """Build a mapping of tokens to the number of granules they appear in"""
        print(f"🔍 Building token to granule mapping for {table_name}...")
        
        # Query to get granule information for each sentence
        granule_query = f"""
        SELECT 
            sentence,
            _part_index,
            intDiv(rowNumberInAllBlocks(), 512) as granule_id
        FROM {table_name}
        ORDER BY _part_index, granule_id
        """
        
        result, success = self.execute_query(granule_query)
        if not success:
            print(f"❌ Failed to get granule mapping: {result}")
            return {}
        
        token_granule_map = {}
        
        # Process each sentence and track which granules contain which tokens
        for line in result.strip().split('\n'):
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) >= 3:
                sentence = parts[0]
                part_index = int(parts[1])
                granule_id = int(parts[2])
                granule_key = f"{part_index}_{granule_id}"
                
                # Extract tokens from this sentence
                words = sentence.split()
                for word in words:
                    token = word.strip('.,!?;:"()[]{}')
                    if token:
                        if token not in token_granule_map:
                            token_granule_map[token] = set()
                        token_granule_map[token].add(granule_key)
        
        # Convert sets to counts
        token_count_map = {token: len(granules) for token, granules in token_granule_map.items()}
        
        print(f"✓ Built token mapping: {len(token_count_map)} unique tokens across granules")
        return token_count_map
    
    def generate_ngram_test_queries(self, num_queries: int = 50, table_nonce: str = None, table_name: str = None) -> List[Tuple[str, str, bool, int]]:
        """Generate n-gram substring queries for sentence data with expected granule counts"""
        queries = []
        
        if not self.inserted_sentences:
            print("⚠️ Warning: No inserted sentences available, generating random queries")
            return queries
        
        # Build n-gram to granule mapping if table_name is provided
        ngram_granule_map = {}
        if table_name:
            ngram_granule_map = self.build_ngram_granule_map(table_name, 3)
        
        for _ in range(num_queries):
            if random.random() < 0.5:  # 50% true positives (substrings from existing sentences)
                sentence = random.choice(self.inserted_sentences)
                # Extract a random substring from the sentence (3-8 characters)
                if len(sentence) > 8:
                    start_idx = random.randint(0, len(sentence) - 8)
                    substring_length = random.randint(3, min(8, len(sentence) - start_idx))
                    target_substring = sentence[start_idx:start_idx + substring_length]
                else:
                    target_substring = sentence[:min(len(sentence), 5)]
                should_exist = True
            else:  # 50% false positives (random substrings)
                target_substring = self.generate_random_string(random.randint(3, 8))
                should_exist = False
            
            # Get expected granule count from the mapping
            expected_granules = ngram_granule_map.get(target_substring, 0) if ngram_granule_map else 0
            
            # Escape single quotes in the substring
            escaped_substring = target_substring.replace("'", "\\'")
            nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
            query = f"SELECT COUNT(*) FROM {{table}} WHERE match(sentence, '{escaped_substring}') SETTINGS force_data_skipping_indices='idx_sentence'{nonce_comment}"
            queries.append((query, target_substring, should_exist, expected_granules))
            
        print(f"📊 Generated {num_queries} n-gram queries: {sum(1 for _, _, exists, _ in queries if exists)} true positives, {sum(1 for _, _, exists, _ in queries if not exists)} false positives")
        return queries

    def generate_token_test_queries(self, num_queries: int = 50, table_nonce: str = None, table_name: str = None) -> List[Tuple[str, str, bool, int]]:
        """Generate token-based queries for sentence data with expected granule counts"""
        queries = []
        
        if not self.inserted_sentences:
            print("⚠️ Warning: No inserted sentences available, generating random queries")
            return queries
        
        # Build token to granule mapping if table_name is provided
        token_granule_map = {}
        if table_name:
            token_granule_map = self.build_token_granule_map(table_name)
        
        for _ in range(num_queries):
            if random.random() < 0.5:  # 50% true positives (tokens from existing sentences)
                sentence = random.choice(self.inserted_sentences)
                # Extract a random word/token from the sentence
                words = sentence.split()
                if words:
                    target_token = random.choice(words).strip('.,!?;:"()[]{}')
                else:
                    target_token = "the"  # fallback
                should_exist = True
            else:  # 50% false positives (random tokens)
                target_token = self.generate_random_string(random.randint(4, 10))
                should_exist = False
            
            # Get expected granule count from the mapping
            expected_granules = token_granule_map.get(target_token, 0) if token_granule_map else 0
            
            # Escape single quotes in the token
            escaped_token = target_token.replace("'", "\\'")
            nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
            query = f"SELECT COUNT(*) FROM {{table}} WHERE hasToken(sentence, '{escaped_token}') SETTINGS force_data_skipping_indices='idx_sentence'{nonce_comment}"
            queries.append((query, target_token, should_exist, expected_granules))
            
        print(f"📊 Generated {num_queries} token queries: {sum(1 for _, _, exists, _ in queries if exists)} true positives, {sum(1 for _, _, exists, _ in queries if not exists)} false positives")
        return queries
    
    def generate_random_string(self, length: int = None) -> str:
        """Generate a random string for testing false positives"""
        if length is None:
            # Generate random length between 3 and 15 characters
            length = random.randint(3, 15)
        
        # Use letters and digits for random string generation
        characters = string.ascii_lowercase
        return ''.join(random.choice(characters) for _ in range(length))
    
    def run_query_performance_test(self, table_name: str, queries: List[Tuple[str, str, bool, int]], iterations: int = 1, table_nonce: str = None) -> Dict:
        """Run performance test on queries and collect metrics with expected granule comparison"""
        results = {
            'table_name': table_name,
            'total_queries': len(queries) * iterations,
            'execution_times': [],
            'index_usage': {'idx_sentence': []},
            'granules_examined': [],
            'query_details': [],  # Store detailed results per query
            'table_nonce': table_nonce  # Store table-specific nonce instead of global
        }
        
        print(f"🔄 Running {len(queries)} queries {iterations} times on {table_name}...")
        
        # Get baseline filtering marks metric before starting
        baseline_filtering_marks = self.get_filtering_marks_metric()
        print(f"📊 Baseline FilteringMarksWithSecondaryKeysMicroseconds: {baseline_filtering_marks}")
        
        # Record start time for this test batch
        batch_start_time = time.time()
        
        for iteration in range(iterations):
            for i, query_data in enumerate(queries):
                # Handle both old format (3 elements) and new format (4 elements)
                if len(query_data) == 4:
                    query_template, target_word, should_exist, expected_granules = query_data
                else:
                    query_template, target_word, should_exist = query_data
                    expected_granules = 0  # Default for old format
                
                query = query_template.format(table=table_name)
                
                # Run EXPLAIN to get index usage
                explain_query = f"EXPLAIN indexes = 1 {query}"
                
                # Execute actual query
                result_output, success = self.execute_query(query)
                
                if success:
                    # Get explain results
                    explain_output, explain_success = self.execute_query(explain_query)
                    
                    granules_examined = 0
                    excessive_granules = 0
                    false_positive_ratio = 0.0
                    index_efficiency = 0.0
                    
                    if explain_success:
                        # Parse index usage from explain
                        sentence_usage = self.parse_index_usage(explain_output, 'idx_sentence')
                        results['index_usage']['idx_sentence'].append(sentence_usage)
                        
                        # Calculate granules examined vs expected
                        total_granules = sentence_usage.get('total_granules', 0)
                        scanned_granules = sentence_usage.get('scanned_granules', 0)  # Actually scanned granules
                        
                        print(f"    Query target='{target_word}', should_exist={should_exist}")
                        print(f"    Total granules: {total_granules}, Scanned: {scanned_granules}, Expected: {expected_granules}")

                        # Calculate excessive granules based on expected vs actual
                        if expected_granules > 0:
                            # We have expected granule count - use it for comparison
                            excessive_granules = max(0, scanned_granules - expected_granules)
                            index_efficiency = expected_granules / scanned_granules if scanned_granules > 0 else 1.0
                        else:
                            # Fallback to old logic for compatibility
                            if should_exist:
                                expected_granules = 1  # Assume at least 1 granule should contain the data
                                excessive_granules = max(0, scanned_granules - expected_granules)
                            else:
                                expected_granules = 0  # For non-existing data, expect 0 granules
                                excessive_granules = scanned_granules
                        
                        # Calculate false positive ratio: excessive granules / total granules
                        if total_granules > 0:
                            false_positive_ratio = excessive_granules / total_granules
                        else:
                            false_positive_ratio = 0.0
                        
                        print(f"    Expected: {expected_granules}, Excessive: {excessive_granules}, FP Ratio: {false_positive_ratio:.3f}, Efficiency: {index_efficiency:.3f}")
                        
                        if excessive_granules > 0:
                            print(f"    INEFFICIENT: {excessive_granules} excessive granules out of {total_granules} total")
                        else:
                            print(f"    OPTIMAL: Index filtering worked perfectly")
                    
                    results['granules_examined'].append(scanned_granules)
                    
                    # Store detailed query information with expected granule comparison
                    results['query_details'].append({
                        'target_word': target_word,
                        'should_exist': should_exist,
                        'expected_granules': expected_granules,
                        'granules_examined': scanned_granules,
                        'excessive_granules': excessive_granules,
                        'false_positive_ratio': false_positive_ratio,
                        'index_efficiency': index_efficiency,
                        'index_usage': sentence_usage
                    })
                        
                else:
                    print(f"✗ Query failed: {result_output}")
                    print(query)
                    results['granules_examined'].append(0)
                    results['query_details'].append({
                        'target_word': target_word,
                        'should_exist': should_exist,
                        'expected_granules': expected_granules,
                        'granules_examined': 0,
                        'excessive_granules': 0,
                        'false_positive_ratio': 0.0,
                        'index_efficiency': 0.0,
                        'index_usage': {}
                    })
        
        # Wait a moment for query_log to be updated
        time.sleep(2)
        
        # Get execution times from system.query_log
        execution_times = self.get_execution_times_from_query_log(batch_start_time, iterations*len(queries), table_name, table_nonce)
        results['execution_times'] = execution_times
        
        # Calculate aggregated metrics
        valid_times = [t for t in results['execution_times'] if t > 0]
        results['avg_execution_time'] = sum(valid_times) / len(valid_times) if valid_times else 0
        results['min_execution_time'] = min(valid_times) if valid_times else 0
        results['max_execution_time'] = max(valid_times) if valid_times else 0
        results['throughput_qps'] = len(valid_times) / sum(valid_times) if sum(valid_times) > 0 else 0
        results['avg_granules_examined'] = sum(results['granules_examined']) / len(results['granules_examined']) if results['granules_examined'] else 0
        
        # Calculate granule-based false positive metrics only
        fp_ratios = [detail['false_positive_ratio'] for detail in results['query_details']]
        excessive_granules = [detail['excessive_granules'] for detail in results['query_details']]
        
        # Calculate total granules across all queries
        total_granules_all_queries = sum([detail['index_usage'].get('total_granules', 0) for detail in results['query_details']])
        total_excessive_granules = sum(excessive_granules)
        
        # Primary false positive rate: total excessive granules / total granules across all queries
        results['false_positive_rate'] = total_excessive_granules / total_granules_all_queries if total_granules_all_queries > 0 else 0.0
        results['avg_false_positive_ratio'] = sum(fp_ratios) / len(fp_ratios) if fp_ratios else 0.0
        results['max_false_positive_ratio'] = max(fp_ratios) if fp_ratios else 0.0
        results['total_excessive_granules'] = total_excessive_granules
        results['total_granules_examined'] = total_granules_all_queries
        results['avg_excessive_granules'] = sum(excessive_granules) / len(excessive_granules) if excessive_granules else 0.0
        
        # Get final filtering marks metric and calculate the difference
        final_filtering_marks = self.get_filtering_marks_metric()
        filtering_marks_delta = final_filtering_marks - baseline_filtering_marks
        results['filtering_marks_microseconds'] = filtering_marks_delta
        results['avg_filtering_marks_per_query'] = filtering_marks_delta / results['total_queries'] if results['total_queries'] > 0 else 0
        
        print(f"✓ Completed performance test for {table_name}")
        print(f"  False Positive Rate: {results['false_positive_rate']:.4f} ({total_excessive_granules}/{total_granules_all_queries} excessive/total)")
        print(f"  Filtering marks time: {filtering_marks_delta}μs total, {results['avg_filtering_marks_per_query']:.1f}μs avg per query")
        print(f"  Avg granules examined: {results['avg_granules_examined']:.2f}")
        print(f"  Avg FP ratio per query: {results['avg_false_positive_ratio']:.3f}, Max: {results['max_false_positive_ratio']:.3f}")
        print(f"  Total excessive granules: {results['total_excessive_granules']}, Avg excessive: {results['avg_excessive_granules']:.2f}")
        return results
    
    def parse_index_usage(self, explain_text: str, index_name: str) -> Dict:

        print(f"🔍 Parsing index usage for {index_name}")
        print("Explaining query plan...")
        print(explain_text)

        # Look for the new format:
        # Skip
        #   Name: idx_id
        #   Description: surf_filter GRANULARITY 1
        #   Parts: 0/1
        #   Granules: 0/122
        
        lines = explain_text.split('\n')
        found_skip_section = False
        found_target_index = False
        
        for i, line in enumerate(lines):
            # Look for "Skip" section
            if "Skip" in line and not found_skip_section:
                found_skip_section = True
                print(f"    Found Skip section at line: {line}")
                continue
                
            # If we're in Skip section, look for our index name
            if found_skip_section and f"Name: {index_name}" in line:
                found_target_index = True
                print(f"    Found target index {index_name} at line: {line}")
                continue
                
            # If we found our index, look for the Granules line
            if found_target_index and "Granules:" in line:
                print(f"    Found granules line: {line}")
                try:
                    # Extract granules information from "Granules: 0/122"
                    granules_part = line.split("Granules:")[1].strip()
                    if "/" in granules_part:
                        scanned, total = map(int, granules_part.split("/"))
                        result = {
                            'scanned_granules': scanned,  # Granules scanned by the index
                            'total_granules': total           # Total granules in the table
                        }
                        print(f"    Parsed result: {result}")
                        return result
                except (ValueError, IndexError) as e:
                    print(f"    Error parsing granules line: {e}")
                    
            # Reset if we hit another Skip section or major section
            if "Skip" in line and found_skip_section:
                found_skip_section = False
                found_target_index = False
        
        # Also try the old format as fallback
        if f"Index `{index_name}`" in explain_text:
            print(f"    Trying old format for index {index_name}")
            for line in lines:
                if f"Index `{index_name}`" in line and "granules" in line:
                    print(f"    Found old format granules line: {line}")
                    try:
                        parts = line.split()
                        for part in parts:
                            if "/" in part and part.replace("/", "").isdigit():
                                scanned, total = map(int, part.split("/"))
                                result = {
                                    'scanned_granules': scanned,  # Granules filtered out by the index
                                    'total_granules': total           # Total granules in the table
                                }
                                print(f"    Parsed old format result: {result}")
                                return result
                    except (ValueError, IndexError) as e:
                        print(f"    Error parsing old format line: {e}")

        default_result = {'scanned_granules': 0, 'total_granules': 0}
        print(f"    Returning default result: {default_result}")
        return default_result

    def get_execution_times_from_query_log(self, start_time: float, limit: int, table_name: str, table_nonce: str = None) -> List[float]:
        """Get execution times from system.query_log using table-specific nonce filtering"""
        # Convert start_time to ClickHouse format
        start_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_time))
        
        # Use table-specific nonce if provided, otherwise fall back to table name only
        nonce_filter = f" AND query LIKE '%nonce:{table_nonce}%'" if table_nonce else ""
        
        query_log_query = f"""
        SELECT query_duration_ms / 1000.0 as execution_time
        FROM system.query_log 
        WHERE query LIKE '%{table_name}%'
          AND type = 'QueryFinish'
          AND event_time >= '{start_datetime}'
          AND query NOT LIKE '%EXPLAIN%'
          {nonce_filter}
        ORDER BY event_time DESC
        LIMIT {limit}
        """
        
        result_output, success = self.execute_query(query_log_query)
        execution_times = []
        
        if success and result_output:
            for line in result_output.strip().split('\n'):
                if line.strip():
                    try:
                        exec_time = float(line.strip())
                        execution_times.append(exec_time)
                    except ValueError:
                        continue
        
        return execution_times
    
    def get_index_creation_time_from_query_log(self, start_time: float, table_name: str, table_nonce: str = None, query_type: str = "ADD INDEX") -> float:
        """Get index creation time from system.query_log using table-specific nonce filtering"""
        # Convert start_time to ClickHouse format
        start_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_time))
        
        # Use table-specific index creation nonce if provided
        nonce_filter = f" AND query LIKE '%index_creation_nonce:{table_nonce}%'" if table_nonce else ""
        
        # Adjust the query pattern based on query_type
        if query_type == "MATERIALIZE INDEX":
            query_pattern = f"ALTER TABLE {table_name} MATERIALIZE INDEX"
        else:
            query_pattern = f"ALTER TABLE {table_name} ADD INDEX"
        
        query_log_query = f"""
        SELECT query_duration_ms / 1000.0 as execution_time
        FROM system.query_log 
        WHERE query LIKE '%{query_pattern}%'
          AND type = 'QueryFinish'
          AND event_time >= '{start_datetime}'
          {nonce_filter}
        ORDER BY event_time DESC
        LIMIT 1
        """
        
        result_output, success = self.execute_query(query_log_query)
        
        if success and result_output.strip():
            try:
                return float(result_output.strip())
            except ValueError:
                return 0.0
        
        return 0.0
    
    def get_index_sizes(self, table_name: str) -> Dict:
        """Get index size information"""
        size_query = f"""
        SELECT 
            name,
            type,
            data_compressed_bytes,
            data_uncompressed_bytes
        FROM system.data_skipping_indices 
        WHERE database = 'default' 
          AND table = '{table_name}'
        ORDER BY data_compressed_bytes DESC
        """
        
        result_output, success = self.execute_query(size_query)
        sizes = {}
        
        if success and result_output:
            total_compressed = 0
            total_uncompressed = 0
            
            for line in result_output.strip().split('\n'):
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        index_name, index_type = parts[0], parts[1]
                        compressed = int(parts[2]) if parts[2].isdigit() else 0
                        uncompressed = int(parts[3]) if parts[3].isdigit() else 0
                        
                        sizes[index_name] = {
                            'type': index_type,
                            'compressed_bytes': compressed,
                            'uncompressed_bytes': uncompressed,
                            'compression_ratio': compressed / uncompressed if uncompressed > 0 else 0
                        }
                        total_compressed += compressed
                        total_uncompressed += uncompressed
            
            sizes['total'] = {
                'compressed_bytes': total_compressed,
                'uncompressed_bytes': total_uncompressed,
                'compression_ratio': total_compressed / total_uncompressed if total_uncompressed > 0 else 0
            }
        
        return sizes
    
    def format_bytes(self, bytes_val: int) -> str:
        """Format bytes as human readable string"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} TB"
    
    def run_evaluation(self):
        """Run the complete evaluation for n-gram and token strategies"""
        # Give server time to stabilize after startup
        print("⏰ Allowing server startup stabilization...")
        time.sleep(3)
        
        # Test configurations: (strategy_type, n_value_for_ngram)
        test_strategies = [
            ("ngram", 3),  # N-gram with n=3
            ("token", None)  # Token-based (no n parameter)
        ]
        
        results = []
        granularity = 512  # Fixed granularity as specified
        
        for strategy_type, n_value in test_strategies:
            print(f"\n{'='*80}")
            print(f"🚀 Testing Strategy: {strategy_type.upper()}")
            if n_value:
                print(f"   N-gram size: {n_value}")
            print(f"   Granularity: {granularity}")
            print(f"   Configuration: (1024, 3)")
            print(f"{'='*80}")
            
            # Generate separate nonces for each table to prevent metric pollution
            surf_nonce = str(uuid.uuid4()).replace('-', '')[:8]
            bloom_nonce = str(uuid.uuid4()).replace('-', '')[:8]
            
            if strategy_type == "ngram":
                surf_table = f"test_surf_ngram_{surf_nonce}"
                bloom_table = f"test_bloom_ngram_{bloom_nonce}"
            else:  # token
                surf_table = f"test_surf_token_{surf_nonce}"
                bloom_table = f"test_bloom_token_{bloom_nonce}"
            
            print(f"📋 SuRF table: {surf_table}")
            print(f"📋 Bloom table: {bloom_table}")
            
            # Step 1: Delete existing tables
            self.delete_tables_if_exist([surf_table, bloom_table])
            
            # Step 2: Create tables (without indexes)
            if strategy_type == "ngram":
                surf_success = self.create_surf_ngram_table(surf_table, granularity)
                bloom_success = self.create_bloom_ngram_table(bloom_table, granularity)
            else:  # token
                surf_success = self.create_surf_token_table(surf_table, granularity)
                bloom_success = self.create_bloom_token_table(bloom_table, granularity)

            if not (surf_success and bloom_success):
                print(f"✗ Failed to create tables for {strategy_type} strategy")
                continue
            
            # Step 3: Insert sentence data (1 million rows) - same data for both tables
            self.insert_sentence_data(surf_table, 1000000)
            self.insert_sentence_data(bloom_table, 1000000)
            
            # Step 4: Create indexes and measure creation time
            if strategy_type == "ngram":
                surf_construction_time = self.create_surf_ngram_index(surf_table, n_value, surf_nonce)
                bloom_construction_time = self.create_bloom_ngram_index(bloom_table, n_value, bloom_nonce)
            else:  # token
                surf_construction_time = self.create_surf_token_index(surf_table, surf_nonce)
                bloom_construction_time = self.create_bloom_token_index(bloom_table, bloom_nonce)
            
            # Restart ClickHouse server after data insertion to test persistence
            print("🔄 Restarting ClickHouse server after data insertion...")
            self.restart_clickhouse_server()
            
            # Step 5: Generate test queries based on strategy
            if strategy_type == "ngram":
                surf_test_queries = self.generate_ngram_test_queries(50, surf_nonce, surf_table)
                bloom_test_queries = self.generate_ngram_test_queries(50, bloom_nonce, bloom_table)
            else:  # token
                surf_test_queries = self.generate_token_test_queries(50, surf_nonce, surf_table)
                bloom_test_queries = self.generate_token_test_queries(50, bloom_nonce, bloom_table)
            
            # Step 6: Run performance tests
            surf_results = self.run_query_performance_test(surf_table, surf_test_queries, 1, surf_nonce)
            bloom_results = self.run_query_performance_test(bloom_table, bloom_test_queries, 1, bloom_nonce)
            
            # Step 7: Get index sizes
            surf_sizes = self.get_index_sizes(surf_table)
            bloom_sizes = self.get_index_sizes(bloom_table)
            
            # Compile results
            strategy_results = {
                'strategy': strategy_type,
                'n_value': n_value,
                'granularity': granularity,
                'configuration': "(512, 3)",
                'surf': {
                    'performance': surf_results,
                    'sizes': surf_sizes,
                    'construction_time_seconds': surf_construction_time
                },
                'bloom': {
                    'performance': bloom_results,
                    'sizes': bloom_sizes,
                    'construction_time_seconds': bloom_construction_time
                }
            }
            
            results.append(strategy_results)
            
            # Print intermediate results
            self.print_strategy_results(strategy_results)
            
            print(f"✅ Completed {strategy_type} strategy evaluation")
        
        # Final results summary
        print(f"\n{'='*80}")
        print("📊 FINAL RESULTS SUMMARY")
        print(f"{'='*80}")
        
        self.print_final_summary(results)
        
        # Save results to JSON file
        timestamp = int(time.time())
        output_file = f"surf_vs_bloom_ngram_token_results_{timestamp}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"💾 Results saved to {output_file}")
        except Exception as e:
            print(f"⚠️ Could not save results: {e}")
        
        return results
            
            # Cleanup tables to save space - DISABLED to keep tables for analysis
            # self.delete_tables_if_exist([surf_table, bloom_table])
        
        # Print final comparison
        self.print_final_results(results)
        
        return results
    
    def print_config_results(self, config_results: Dict):
        """Print results for a single configuration"""
        config = config_results['config']
        surf = config_results['surf']
        bloom = config_results['bloom']
        
        print(f"\n📊 Results for {config}:")
        print(f"{'─'*50}")
        
        # Performance comparison
        print("🚀 Performance Metrics:")
        surf_latency_ms = surf['performance']['avg_execution_time'] * 1000
        bloom_latency_ms = bloom['performance']['avg_execution_time'] * 1000
        
        print(f"  SuRF   - Latency: {surf_latency_ms:.2f}ms, "
              f"Throughput: {surf['performance']['throughput_qps']:.1f} QPS, "
              f"Avg Granules: {surf['performance']['avg_granules_examined']:.1f}")
        print(f"  Bloom  - Latency: {bloom_latency_ms:.2f}ms, "
              f"Throughput: {bloom['performance']['throughput_qps']:.1f} QPS, "
              f"Avg Granules: {bloom['performance']['avg_granules_examined']:.1f}")
        
        # Granule efficiency comparison
        print("\n🎯 Granule Efficiency:")
        surf_total_granules = surf['performance'].get('total_granules_examined', 0)
        bloom_total_granules = bloom['performance'].get('total_granules_examined', 0)
        surf_excessive = surf['performance'].get('total_excessive_granules', 0)
        bloom_excessive = bloom['performance'].get('total_excessive_granules', 0)
        
        print(f"  SuRF   - FP Rate: {surf['performance']['false_positive_rate']:.4f} ({surf_excessive}/{surf_total_granules} excessive/total)")
        print(f"  Bloom  - FP Rate: {bloom['performance']['false_positive_rate']:.4f} ({bloom_excessive}/{bloom_total_granules} excessive/total)")
        
        # Filtering marks comparison  
        print("\n⚡ Index Filtering Performance:")
        surf_filtering_avg = surf['performance'].get('avg_filtering_marks_per_query', 0)
        bloom_filtering_avg = bloom['performance'].get('avg_filtering_marks_per_query', 0)
        print(f"  SuRF   - Avg filtering time: {surf_filtering_avg:.1f}μs per query")
        print(f"  Bloom  - Avg filtering time: {bloom_filtering_avg:.1f}μs per query")
        
        # Size comparison
        print("\n💾 Index Sizes:")
        if 'total' in surf['sizes']:
            print(f"  SuRF   - Compressed: {self.format_bytes(surf['sizes']['total']['compressed_bytes'])}, "
                  f"Uncompressed: {self.format_bytes(surf['sizes']['total']['uncompressed_bytes'])}")
        if 'total' in bloom['sizes']:
            print(f"  Bloom  - Compressed: {self.format_bytes(bloom['sizes']['total']['compressed_bytes'])}, "
                  f"Uncompressed: {self.format_bytes(bloom['sizes']['total']['uncompressed_bytes'])}")
    
    def print_final_results(self, all_results: List[Dict]):
        """Print comprehensive final results"""
        print(f"\n{'='*80}")
        print("🏆 FINAL EVALUATION RESULTS")
        print(f"{'='*80}")
        
        # Create summary table header (comprehensive performance metrics + index sizes + filtering marks)
        print(f"{'Config':<20} {'SuRF Lat(ms)':<11} {'Bloom Lat(ms)':<12} {'SuRF QPS':<9} {'Bloom QPS':<10} {'SuRF FP Rate':<11} {'Bloom FP Rate':<12} {'SuRF Gran':<9} {'Bloom Gran':<10} {'SuRF Filt(μs)':<12} {'Bloom Filt(μs)':<14} {'SuRF Comp(KB)':<12} {'SuRF Uncomp(KB)':<14} {'Bloom Comp(KB)':<14} {'Bloom Uncomp(KB)':<16}")
        print("─" * 230)
        
        # Create summary data
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            bloom_perf = result['bloom']['performance']
            surf_sizes = result['surf']['sizes']
            bloom_sizes = result['bloom']['sizes']
            
            # Convert latency from seconds to milliseconds
            surf_latency_ms = surf_perf['avg_execution_time'] * 1000
            bloom_latency_ms = bloom_perf['avg_execution_time'] * 1000
            
            # Get index sizes in KB (both compressed and uncompressed)
            surf_comp_kb = surf_sizes.get('total', {}).get('compressed_bytes', 0) / 1024
            surf_uncomp_kb = surf_sizes.get('total', {}).get('uncompressed_bytes', 0) / 1024
            bloom_comp_kb = bloom_sizes.get('total', {}).get('compressed_bytes', 0) / 1024
            bloom_uncomp_kb = bloom_sizes.get('total', {}).get('uncompressed_bytes', 0) / 1024
            
            # Get filtering marks average per query
            surf_filtering_avg = surf_perf.get('avg_filtering_marks_per_query', 0)
            bloom_filtering_avg = bloom_perf.get('avg_filtering_marks_per_query', 0)
            
            print(f"{config:<20} "
                  f"{surf_latency_ms:<11.2f} "
                  f"{bloom_latency_ms:<12.2f} "
                  f"{surf_perf['throughput_qps']:<9.1f} "
                  f"{bloom_perf['throughput_qps']:<10.1f} "
                  f"{surf_perf['false_positive_rate']:<11.4f} "
                  f"{bloom_perf['false_positive_rate']:<12.4f} "
                  f"{surf_perf['avg_granules_examined']:<9.1f} "
                  f"{bloom_perf['avg_granules_examined']:<10.1f} "
                  f"{surf_filtering_avg:<12.1f} "
                  f"{bloom_filtering_avg:<14.1f} "
                  f"{surf_comp_kb:<12.1f} "
                  f"{surf_uncomp_kb:<14.1f} "
                  f"{bloom_comp_kb:<14.1f} "
                  f"{bloom_uncomp_kb:<16.1f}")
        
        # Print detailed false positive analysis
        print(f"\n📈 False Positive Ratio Analysis:")
        print(f"{'Config':<20} {'SuRF Avg FP Ratio':<16} {'Bloom Avg FP Ratio':<18} {'SuRF Max FP Ratio':<16} {'Bloom Max FP Ratio':<18}")
        print("─" * 90)
        
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            bloom_perf = result['bloom']['performance']
            
            print(f"{config:<20} "
                  f"{surf_perf['avg_false_positive_ratio']:<16.3f} "
                  f"{bloom_perf['avg_false_positive_ratio']:<18.3f} "
                  f"{surf_perf['max_false_positive_ratio']:<16.3f} "
                  f"{bloom_perf['max_false_positive_ratio']:<18.3f}")
        
        # Print excessive granule analysis
        print(f"\n🔍 Excessive Granule Analysis:")
        print(f"{'Config':<20} {'SuRF Total Excessive':<19} {'Bloom Total Excessive':<21} {'SuRF Avg Excessive':<17} {'Bloom Avg Excessive':<19}")
        print("─" * 100)
        
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            bloom_perf = result['bloom']['performance']
            
            print(f"{config:<20} "
                  f"{surf_perf['total_excessive_granules']:<19} "
                  f"{bloom_perf['total_excessive_granules']:<21} "
                  f"{surf_perf['avg_excessive_granules']:<17.2f} "
                  f"{bloom_perf['avg_excessive_granules']:<19.2f}")
        
        # Print index construction time analysis
        print(f"\n⏱️ Index Construction Time Analysis:")
        print(f"{'Config':<20} {'SuRF Construction (s)':<20} {'Bloom Construction (s)':<22} {'Speedup (Bloom/SuRF)':<20}")
        print("─" * 85)
        
        for result in all_results:
            config = result['config']
            surf_time = result['surf'].get('construction_time_seconds', 0)
            bloom_time = result['bloom'].get('construction_time_seconds', 0)
            speedup = bloom_time / surf_time if surf_time > 0 else 0
            
            print(f"{config:<20} "
                  f"{surf_time:<20.3f} "
                  f"{bloom_time:<22.3f} "
                  f"{speedup:<20.2f}x")

    def print_strategy_results(self, strategy_results):
        """Print results for a specific strategy (ngram or token)"""
        strategy = strategy_results['strategy']
        surf_perf = strategy_results['surf']['performance']
        bloom_perf = strategy_results['bloom']['performance']
        surf_sizes = strategy_results['surf']['sizes']
        bloom_sizes = strategy_results['bloom']['sizes']
        
        # Convert seconds to milliseconds for latency display
        surf_latency_ms = surf_perf.get('avg_execution_time', 0) * 1000
        bloom_latency_ms = bloom_perf.get('avg_execution_time', 0) * 1000
        
        print(f"\n📊 {strategy.upper()} Strategy Results:")
        print(f"   SuRF  - Avg latency: {surf_latency_ms:.2f}ms, "
              f"FP ratio: {surf_perf.get('avg_false_positive_ratio', 0):.3f}")
        print(f"   Bloom - Avg latency: {bloom_latency_ms:.2f}ms, "
              f"FP ratio: {bloom_perf.get('avg_false_positive_ratio', 0):.3f}")
        
        surf_construction = strategy_results['surf'].get('construction_time_seconds', 0)
        bloom_construction = strategy_results['bloom'].get('construction_time_seconds', 0)
        print(f"   Construction time - SuRF: {surf_construction:.3f}s, Bloom: {bloom_construction:.3f}s")
        
        # Index size comparison
        surf_comp_bytes = surf_sizes.get('total', {}).get('compressed_bytes', 0)
        bloom_comp_bytes = bloom_sizes.get('total', {}).get('compressed_bytes', 0)
        surf_uncomp_bytes = surf_sizes.get('total', {}).get('uncompressed_bytes', 0)
        bloom_uncomp_bytes = bloom_sizes.get('total', {}).get('uncompressed_bytes', 0)
        
        print(f"   Index sizes:")
        print(f"     SuRF  - Compressed: {self.format_bytes(surf_comp_bytes)}, Uncompressed: {self.format_bytes(surf_uncomp_bytes)}")
        print(f"     Bloom - Compressed: {self.format_bytes(bloom_comp_bytes)}, Uncompressed: {self.format_bytes(bloom_uncomp_bytes)}")
        
        # Size efficiency comparison
        if surf_comp_bytes > 0 and bloom_comp_bytes > 0:
            size_ratio = bloom_comp_bytes / surf_comp_bytes
            print(f"     Size ratio (Bloom/SuRF): {size_ratio:.2f}x")

    def print_final_summary(self, all_results):
        """Print final summary of all results"""
        print(f"\n📈 Performance Summary:")
        print(f"{'Strategy':<10} {'SuRF Latency (ms)':<16} {'Bloom Latency (ms)':<18} {'SuRF FP Ratio':<14} {'Bloom FP Ratio':<16}")
        print("─" * 80)
        
        for result in all_results:
            strategy = result['strategy']
            surf_perf = result['surf']['performance']
            bloom_perf = result['bloom']['performance']
            
            # Convert seconds to milliseconds
            surf_latency_ms = surf_perf.get('avg_execution_time', 0) * 1000
            bloom_latency_ms = bloom_perf.get('avg_execution_time', 0) * 1000
            
            print(f"{strategy:<10} "
                  f"{surf_latency_ms:<16.2f} "
                  f"{bloom_latency_ms:<18.2f} "
                  f"{surf_perf.get('avg_false_positive_ratio', 0):<14.3f} "
                  f"{bloom_perf.get('avg_false_positive_ratio', 0):<16.3f}")
        
        # Add index size comparison summary
        print(f"\n💾 Index Size Summary:")
        print(f"{'Strategy':<10} {'SuRF Compressed':<16} {'Bloom Compressed':<18} {'SuRF Uncompressed':<18} {'Bloom Uncompressed':<20}")
        print("─" * 90)
        
        for result in all_results:
            strategy = result['strategy']
            surf_sizes = result['surf']['sizes']
            bloom_sizes = result['bloom']['sizes']
            
            surf_comp = surf_sizes.get('total', {}).get('compressed_bytes', 0)
            bloom_comp = bloom_sizes.get('total', {}).get('compressed_bytes', 0)
            surf_uncomp = surf_sizes.get('total', {}).get('uncompressed_bytes', 0)
            bloom_uncomp = bloom_sizes.get('total', {}).get('uncompressed_bytes', 0)
            
            print(f"{strategy:<10} "
                  f"{self.format_bytes(surf_comp):<16} "
                  f"{self.format_bytes(bloom_comp):<18} "
                  f"{self.format_bytes(surf_uncomp):<18} "
                  f"{self.format_bytes(bloom_uncomp):<20}")
        
        print(f"\n⏱️ Construction Time Summary:")
        print(f"{'Strategy':<10} {'SuRF Time (s)':<14} {'Bloom Time (s)':<16} {'Ratio (B/S)':<12}")
        print("─" * 55)
        
        for result in all_results:
            strategy = result['strategy']
            surf_time = result['surf'].get('construction_time_seconds', 0)
            bloom_time = result['bloom'].get('construction_time_seconds', 0)
            ratio = bloom_time / surf_time if surf_time > 0 else 0
            
            print(f"{strategy:<10} "
                  f"{surf_time:<14.3f} "
                  f"{bloom_time:<16.3f} "
                  f"{ratio:<12.2f}")

def main():
    parser = argparse.ArgumentParser(description='SuRF vs Bloom Filter Performance Evaluation - N-gram and Token Strategies')
    parser.add_argument('--client-path', default='./build/programs/clickhouse', 
                       help='Path to ClickHouse client binary')
    
    args = parser.parse_args()
    
    print("🎯 Starting SuRF vs Bloom Filter Evaluation (N-gram and Token Strategies)")
    print("📝 Testing: ngramsf_v1 vs ngrambf_v1 and tokensf_v1 vs tokenbf_v1")
    print("📊 Data source: sentences.txt (first 1 million entries)")
    print("⚙️ Configuration: (512, 3) for all tests")
    print()
    
    evaluator = ClickHouseIndexEvaluator(args.client_path)
    
    try:
        # Start ClickHouse server
        if not evaluator.start_clickhouse_server():
            print("❌ Failed to start ClickHouse server")
            return 1
        
        # Run the evaluation
        results = evaluator.run_evaluation()
        
        if results:
            print("\n🎉 Evaluation completed successfully!")
            print(f"📊 Tested {len(results)} strategies")
        else:
            print("\n⚠️ No results obtained")
            return 1
    
    except KeyboardInterrupt:
        print("\n⚠️ Evaluation interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        return 1
    finally:
        # Always stop the server
        evaluator.stop_clickhouse_server()
        print("👋 Goodbye!")
    
    return 0

if __name__ == "__main__":
    exit(main())
    print(f"   Using ClickHouse client: {args.client_path}")
    print("   Test data: Words from words.txt file")
    print("   Query type: Point queries on string field")
    print("   Index granularity: 100 (fixed)")
    
    try:
        evaluator = ClickHouseIndexEvaluator(args.client_path)
        
        # Start ClickHouse server at the beginning
        print("🚀 Starting ClickHouse server...")
        evaluator.start_clickhouse_server()
        
        results = evaluator.run_evaluation()
        print("\n✅ Evaluation completed successfully!")
        
        # Gracefully stop the server at the end
        print("🛑 Stopping ClickHouse server...")
        evaluator.stop_clickhouse_server()
        
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to stop server even if evaluation failed
        try:
            if 'evaluator' in locals():
                print("🛑 Attempting to stop ClickHouse server after failure...")
                evaluator.stop_clickhouse_server()
        except:
            pass

if __name__ == "__main__":
    main()
