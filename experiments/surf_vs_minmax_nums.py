#!/usr/bin/env python3
"""
SuRF vs Minmax Filter Performance Evaluation Script - String Range Queries

This script compares the performance of SuRF vs Minmax filters using their optimal query patterns:
- SuRF tables: IN queries (word IN ('start','end')) - acts as BETWEEN 'start' AND 'end' for SuRF filters
- MinMax tables: BETWEEN queries (word BETWEEN 'start' AND 'end') - standard range filtering
Uses words.txt dataset with string data and differentiated query workloads.
"""

import subprocess
import random
import time
import json
import argparse
import signal
import os
from unittest import case
from typing import List, Dict, Tuple
import uuid

class ClickHouseIndexEvaluator:
    def __init__(self, clickhouse_client_path='./build/programs/clickhouse'):
        """Initialize with ClickHouse client path"""
        self.client_path = clickhouse_client_path
        self.server_path = clickhouse_client_path
        self.server_process = None
        # Remove global nonce - use table-specific nonces instead
        self.inserted_words = set()  # Store inserted words for query generation
        print(f"🎯 ClickHouse Index Evaluator initialized")
        
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
                time.sleep(2)
                print(f"   Attempt {attempt + 1}/10 - waiting for server...")
            
            print("❌ Failed to connect to ClickHouse server")
            return False
            
        except Exception as e:
            print(f"❌ Error starting server: {e}")
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

    def create_surf_table(self, table_name: str, granularity: int) -> bool:
        """Create table without index for string data"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            word String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created table {table_name} (without index)")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            print(create_sql)
            return False

    def create_minmax_table(self, table_name: str, granularity: int) -> bool:
        """Create table without index for string data"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            word String
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created table {table_name} (without index)")
            return True
        else:
            print(f"✗ Error creating table {table_name}: {result}")
            print(create_sql)
            return False
    
    def create_surf_index(self, table_name: str, approx_fp_rate: float, table_nonce: str = None) -> float:
        """Create SuRF index on existing table and measure creation time
        Returns: index creation time in seconds"""
        print(f"🔄 Creating SuRF index on {table_name}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_word word TYPE surf_filter({approx_fp_rate}) GRANULARITY 1
        """
        
        print("📝 Adding SuRF index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding SuRF index definition: {result}")
            return 0.0
        
        print("✓ SuRF index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting SuRF index materialization timing...")
        
        # Record start time for measuring materialization
        start_time = time.time()
        
        # Add nonce comment to the query for identification in query_log
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_word{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
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
    
    def create_minmax_index(self, table_name: str, approx_fp_rate: float, table_nonce: str = None) -> float:
        """Create Minmax index on existing table and measure creation time
        Returns: index creation time in seconds"""
        print(f"🔄 Creating Minmax index on {table_name}...")
        
        # Step 1: Add the index definition
        create_index_sql = f"""
        ALTER TABLE {table_name} ADD INDEX idx_word word TYPE minmax GRANULARITY 1
        """
        
        print("📝 Adding Minmax index definition...")
        result, success = self.execute_query(create_index_sql)
        
        if not success:
            print(f"✗ Error adding Minmax index definition: {result}")
            return 0.0
        
        print("✓ Minmax index definition added")
        
        # Step 2: Materialize the index and measure the time
        print("⏱️ Starting Minmax index materialization timing...")
        
        # Record start time for measuring materialization
        start_time = time.time()
        
        # Add nonce comment to the query for identification in query_log
        nonce_comment = f" /* index_creation_nonce:{table_nonce} */" if table_nonce else ""
        
        materialize_index_sql = f"""
        ALTER TABLE {table_name} MATERIALIZE INDEX idx_word{nonce_comment}
        """
        
        result, success = self.execute_query(materialize_index_sql)
        end_time = time.time()
        
        if not success:
            print(f"✗ Error materializing Minmax index: {result}")
            return 0.0
        
        # Calculate materialization time
        materialization_time = end_time - start_time
        
        # Also try to get more precise timing from query_log
        time.sleep(2)  # Wait for query_log to be updated
        query_log_time = self.get_index_creation_time_from_query_log(start_time, table_name, table_nonce, "MATERIALIZE INDEX")
        
        # Use query_log time if available, otherwise use our measured time
        creation_time = query_log_time if query_log_time > 0 else materialization_time
        
        print(f"✓ Minmax index materialized in {creation_time:.3f} seconds")
        return creation_time
    
    def insert_test_data(self, table_name: str, num_rows: int = 1000000):
        """Insert string data from words.txt file in random order using ClickHouse File engine"""
        print(f"🔄 Inserting {num_rows} string rows into {table_name} from words.txt in random order...")
        
        # Load words into memory for query generation (only first time)
        if not hasattr(self, 'inserted_words') or not self.inserted_words:
            try:
                with open('user_files/words.txt', 'r') as f:
                    words = []
                    for i, line in enumerate(f):
                        if i >= num_rows:
                            break
                        word = line.strip()
                        if word:  # Skip empty lines
                            words.append(word)
                    
                    # Shuffle words for random insertion order
                    random.shuffle(words)
                    
                    # Store words in memory for query generation
                    self.inserted_words = set(words)
                    print(f"📋 Loaded {len(words)} words into memory for query generation")
                    
                    # Write shuffled words to a temporary file for insertion
                    with open('user_files/words_shuffled.txt', 'w') as shuffle_file:
                        for word in words:
                            shuffle_file.write(f"{word}\n")
                    print(f"📝 Created shuffled words file for random insertion")
                    
            except FileNotFoundError:
                print("❌ Error: words.txt file not found in user_files/ directory")
                return
            except Exception as e:
                print(f"❌ Error reading words file: {e}")
                return
        else:
            print(f"📋 Using previously loaded {len(self.inserted_words)} words for query generation")
        
        # Add delay before insertion
        print("⏳ Delay before insertion...")
        time.sleep(2)
        
        # Create INSERT query using File engine to read directly from shuffled file
        insert_query = f"""
        INSERT INTO {table_name} (word)
        SELECT line as word
        FROM file('words_shuffled.txt', 'LineAsString')
        LIMIT {num_rows}
        """
        
        # Execute insert query using ClickHouse File engine
        print("📤 Inserting data using ClickHouse File engine...")
        result, success = self.execute_query(insert_query)
        
        if not success:
            print(f"✗ Error inserting words: {result}")
            return
        
        print(f"✓ Data insertion completed using File engine")
        
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
    
    def generate_surf_queries(self, num_queries: int = 50, table_nonce: str = None) -> List[Tuple[str, Tuple, bool]]:
        """Generate IN queries for SuRF tables - IN (a,b) acts as BETWEEN a AND b for SuRF filters"""
        queries = []
        used_ranges = set()  # Track used ranges to avoid duplicates
        
        if not self.inserted_words:
            print("⚠️ Warning: No inserted words available, generating random IN range queries")
            # Fallback to random generation if no words are stored
            for _ in range(num_queries):
                # Generate small range boundaries for IN clause (xxx - xxy pattern)
                attempts = 0
                while attempts < 100:  # Prevent infinite loop
                    base_char = random.choice('abcdefghijklmnopqrstuvwxyz')
                    range_start = base_char * 3  # e.g., 'aaa'
                    # Small range: next character + same suffix
                    next_char = chr(min(ord('z'), ord(base_char) + 1))
                    range_end = next_char + base_char + base_char  # e.g., 'baa'
                    
                    range_key = (range_start, range_end)
                    if range_key not in used_ranges:
                        used_ranges.add(range_key)
                        break
                    attempts += 1
                
                should_exist = False
                nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
                query = "SELECT COUNT(*) FROM {table} WHERE word IN ('" + range_start + "', '" + range_end + "') SETTINGS force_data_skipping_indices='idx_word'" + nonce_comment
                queries.append((query, (range_start, range_end), should_exist))
            return queries
        
        # Convert set to list for random sampling and sort for range operations
        inserted_list = sorted(list(self.inserted_words))
        # Filter to only lowercase words for consistent range generation
        lowercase_words = [w for w in inserted_list if w.islower() and w.isalpha()]
        
        for _ in range(num_queries):
            attempts = 0
            while attempts < 100:  # Prevent infinite loop
                if random.random() < 0.5 and lowercase_words:  # 50% true positives (IN ranges containing existing words)
                    # Create an IN range that definitely contains some inserted words
                    if len(lowercase_words) >= 5:
                        start_idx = random.randint(0, len(lowercase_words) - 5)
                        base_word = lowercase_words[start_idx]
                        
                        # Create small range around the word (xxx - xxy pattern)
                        if len(base_word) >= 3:
                            prefix = base_word[:3]
                            # Small range: first 3 chars to first 3 chars with last char incremented
                            range_start = prefix
                            if prefix[2] < 'z':
                                range_end = prefix[:2] + chr(ord(prefix[2]) + 1)
                            else:
                                # If last char is 'z', increment second char if possible
                                if prefix[1] < 'z':
                                    range_end = prefix[0] + chr(ord(prefix[1]) + 1) + 'a'
                                else:
                                    # If both second and last chars are 'z', increment first char if possible
                                    if prefix[0] < 'z':
                                        range_end = chr(ord(prefix[0]) + 1) + 'aa'
                                    else:
                                        # All chars are 'z', use a different range
                                        range_end = 'zza'
                        else:
                            # Fallback for short words
                            range_start = 'abc'
                            range_end = 'abd'
                    else:
                        # Fallback for small datasets
                        range_start = 'cat'
                        range_end = 'cau'
                    should_exist = True
                else:  # 50% false positives (IN ranges with no existing words)
                    # Create small ranges in uncommon letter space
                    base_chars = ['x', 'z', 'q']  # Uncommon starting letters
                    base_char = random.choice(base_chars)
                    second_char = random.choice('yz')
                    third_char = random.choice('xyz')
                    
                    range_start = base_char + second_char + third_char  # e.g., 'xyz'
                    # Small increment with bounds checking
                    if third_char < 'z':
                        range_end = base_char + second_char + chr(ord(third_char) + 1)  # e.g., 'xyz' -> 'xyy'
                    else:
                        # If third char is 'z', increment second char if possible
                        if second_char < 'z':
                            range_end = base_char + chr(ord(second_char) + 1) + 'a'  # e.g., 'xzz' -> 'xya'
                        else:
                            # If both second and third chars are 'z', increment base char if possible
                            if base_char < 'z':
                                range_end = chr(ord(base_char) + 1) + 'aa'  # e.g., 'xzz' -> 'yaa'
                            else:
                                # All chars are 'z', use a different range
                                range_end = 'zza'
                    
                    should_exist = False
                
                # Check for duplicates
                range_key = (range_start, range_end)
                if range_key not in used_ranges:
                    used_ranges.add(range_key)
                    break
                attempts += 1
            
            # Use table-specific nonce instead of global nonce
            # IN (a,b) for SuRF acts as BETWEEN a AND b
            nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
            query = "SELECT COUNT(*) FROM {table} WHERE word IN ('" + range_start + "', '" + range_end + "') SETTINGS force_data_skipping_indices='idx_word'" + nonce_comment
            queries.append((query, (range_start, range_end), should_exist))
            
        print(f"📊 Generated {num_queries} IN range queries: {sum(1 for _, _, exists in queries if exists)} true positives, {sum(1 for _, _, exists in queries if not exists)} false positives")
        return queries

    def generate_minmax_queries(self, num_queries: int = 50, table_nonce: str = None) -> List[Tuple[str, Tuple[str, str], bool]]:
        """Generate BETWEEN queries for MinMax tables - better for range filtering"""
        queries = []
        used_ranges = set()  # Track used ranges to avoid duplicates
        
        if not self.inserted_words:
            print("⚠️ Warning: No inserted words available, generating random BETWEEN queries")
            # Fallback to random generation if no words are stored
            for _ in range(num_queries):
                # Generate small range boundaries (xxx - xxy pattern)
                attempts = 0
                while attempts < 100:  # Prevent infinite loop
                    base_char = random.choice('abcdefghijklmnopqrstuvwxyz')
                    range_start = base_char * 3  # e.g., 'aaa'
                    # Small range: next character + same suffix
                    next_char = chr(min(ord('z'), ord(base_char) + 1))
                    range_end = next_char + base_char + base_char  # e.g., 'baa'
                    
                    range_key = (range_start, range_end)
                    if range_key not in used_ranges:
                        used_ranges.add(range_key)
                        break
                    attempts += 1
                
                should_exist = False
                nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
                query = "SELECT COUNT(*) FROM {table} WHERE word BETWEEN '" + range_start + "' AND '" + range_end + "' SETTINGS force_data_skipping_indices='idx_word'" + nonce_comment
                queries.append((query, (range_start, range_end), should_exist))
            return queries
        
        # Convert set to list for random sampling and sort for range operations
        inserted_list = sorted(list(self.inserted_words))
        # Filter to only lowercase words for consistent range generation
        lowercase_words = [w for w in inserted_list if w.islower() and w.isalpha()]
        
        for _ in range(num_queries):
            attempts = 0
            while attempts < 100:  # Prevent infinite loop
                if random.random() < 0.5 and lowercase_words:  # 50% true positives (ranges containing existing words)
                    # Create a range that definitely contains some inserted words
                    if len(lowercase_words) >= 5:
                        start_idx = random.randint(0, len(lowercase_words) - 5)
                        base_word = lowercase_words[start_idx]
                        
                        # Create small range around the word (xxx - xxy pattern)
                        if len(base_word) >= 3:
                            prefix = base_word[:3]
                            # Small range: first 3 chars to first 3 chars with last char incremented
                            range_start = prefix
                            if prefix[2] < 'z':
                                range_end = prefix[:2] + chr(ord(prefix[2]) + 1)
                            else:
                                # If last char is 'z', increment second char if possible
                                if prefix[1] < 'z':
                                    range_end = prefix[0] + chr(ord(prefix[1]) + 1) + 'a'
                                else:
                                    # If both second and last chars are 'z', increment first char if possible
                                    if prefix[0] < 'z':
                                        range_end = chr(ord(prefix[0]) + 1) + 'aa'
                                    else:
                                        # All chars are 'z', use a different range
                                        range_end = 'zza'
                        else:
                            # Fallback for short words
                            range_start = 'cat'
                            range_end = 'cau'
                    else:
                        # Fallback for small datasets
                        range_start = 'dog'
                        range_end = 'doh'
                    should_exist = True
                else:  # 50% false positives (ranges with no existing words)
                    # Create small ranges in uncommon letter space
                    base_chars = ['x', 'z', 'q']  # Uncommon starting letters
                    base_char = random.choice(base_chars)
                    second_char = random.choice('yz')
                    third_char = random.choice('xyz')
                    
                    range_start = base_char + second_char + third_char  # e.g., 'xyz'
                    # Small increment with bounds checking
                    if third_char < 'z':
                        range_end = base_char + second_char + chr(ord(third_char) + 1)  # e.g., 'xyz' -> 'xyy'
                    else:
                        # If third char is 'z', increment second char if possible
                        if second_char < 'z':
                            range_end = base_char + chr(ord(second_char) + 1) + 'a'  # e.g., 'xzz' -> 'xya'
                        else:
                            # If both second and third chars are 'z', increment base char if possible
                            if base_char < 'z':
                                range_end = chr(ord(base_char) + 1) + 'aa'  # e.g., 'xzz' -> 'yaa'
                            else:
                                # All chars are 'z', use a different range
                                range_end = 'zza'
                    
                    should_exist = False
                
                # Check for duplicates
                range_key = (range_start, range_end)
                if range_key not in used_ranges:
                    used_ranges.add(range_key)
                    break
                attempts += 1
            
            # Use table-specific nonce instead of global nonce
            nonce_comment = f" /* nonce:{table_nonce} */" if table_nonce else ""
            query = "SELECT COUNT(*) FROM {table} WHERE word BETWEEN '" + range_start + "' AND '" + range_end + "' SETTINGS force_data_skipping_indices='idx_word'" + nonce_comment
            queries.append((query, (range_start, range_end), should_exist))
            
        print(f"📊 Generated {num_queries} BETWEEN queries: {sum(1 for _, _, exists in queries if exists)} true positives, {sum(1 for _, _, exists in queries if not exists)} false positives")
        return queries
    
    def run_query_performance_test(self, table_name: str, queries: List[Tuple[str, int, bool]], iterations: int = 1, table_nonce: str = None) -> Dict:
        """Run performance test on queries and collect metrics"""
        results = {
            'table_name': table_name,
            'total_queries': len(queries) * iterations,
            'execution_times': [],
            'index_usage': {'idx_word': []},
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
            for i, (query_template, query_values, should_exist) in enumerate(queries):
                print("Tablename and template")
                print(table_name)
                print(query_template)
                query = query_template.format(table=table_name)
                
                # Handle different query types for logging
                if isinstance(query_values, tuple) and len(query_values) == 2 and all(isinstance(x, str) for x in query_values):
                    range_start, range_end = query_values
                    # Check if this is a BETWEEN query or IN query based on query text
                    if "BETWEEN" in query_template:
                        query_type = "BETWEEN"
                        query_desc = f"'{range_start}'-'{range_end}'"
                    else:
                        # IN query for SuRF (acts as range)
                        query_type = "IN_RANGE"
                        query_desc = f"IN ('{range_start}','{range_end}')"
                else:
                    # Fallback
                    query_type = "UNKNOWN"
                    query_desc = f"({len(query_values) if hasattr(query_values, '__len__') else 1} values)"
                
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
                    
                    if explain_success:
                        # Parse index usage from explain
                        id_usage = self.parse_index_usage(explain_output, 'idx_word')
                        results['index_usage']['idx_word'].append(id_usage)
                        
                        # Calculate granules examined vs expected
                        total_granules = id_usage.get('total_granules', 0)
                        scanned_granules = id_usage.get('scanned_granules', 0)  # Actually scanned granules
                        
                        print(f"    {query_type} query {query_desc}, should_exist={should_exist}")
                        print(f"    Total granules: {total_granules}, Scanned: {scanned_granules}")

                        # Calculate excessive granules and false positive ratio
                        if should_exist:
                            # For queries with expected results, both IN_RANGE and BETWEEN may span multiple granules
                            if query_type == "IN_RANGE":
                                # IN range queries for SuRF should be precise but may span a few granules
                                expected_granules = max(1, min(3, scanned_granules))
                            else:
                                # BETWEEN queries may span multiple granules with MinMax
                                expected_granules = max(1, min(5, scanned_granules))
                            excessive_granules = max(0, scanned_granules - expected_granules)
                        else:
                            # For queries with no expected results, we expect 0 granules to be examined
                            expected_granules = 0
                            excessive_granules = scanned_granules
                        
                        # Calculate false positive ratio: excessive granules / total granules
                        if total_granules > 0:
                            false_positive_ratio = excessive_granules / total_granules
                        else:
                            false_positive_ratio = 0.0
                        
                        print(f"    Expected: {expected_granules}, Excessive: {excessive_granules}, FP Ratio: {false_positive_ratio:.3f}")
                        
                        if excessive_granules > 0:
                            print(f"    INEFFICIENT: {excessive_granules} excessive granules out of {total_granules} total")
                        else:
                            print(f"    OPTIMAL: Index filtering worked perfectly")
                    
                    results['granules_examined'].append(scanned_granules)
                    
                    # Store detailed query information
                    query_detail = {
                        'query_type': query_type,
                        'query_values': query_values,
                        'should_exist': should_exist,
                        'granules_examined': scanned_granules,
                        'excessive_granules': excessive_granules,
                        'false_positive_ratio': false_positive_ratio,
                        'index_usage': id_usage
                    }
                    
                    # Add type-specific details
                    if query_type == "BETWEEN":
                        range_start, range_end = query_values
                        query_detail.update({
                            'range_start': range_start,
                            'range_end': range_end,
                            'range_description': f"BETWEEN '{range_start}' AND '{range_end}'"
                        })
                    elif query_type == "IN_RANGE":
                        range_start, range_end = query_values
                        query_detail.update({
                            'range_start': range_start,
                            'range_end': range_end,
                            'range_description': f"IN ('{range_start}','{range_end}') - acts as range"
                        })
                    else:  # Fallback
                        query_detail.update({
                            'query_description': str(query_values)
                        })
                    
                    results['query_details'].append(query_detail)
                        
                else:
                    print(f"✗ Query failed: {result_output}")
                    results['granules_examined'].append(0)
                    
                    # Create basic failure record
                    query_detail = {
                        'query_type': query_type,
                        'query_values': query_values,
                        'should_exist': should_exist,
                        'granules_examined': 0,
                        'excessive_granules': 0,
                        'false_positive_ratio': 0.0,
                        'index_usage': {}
                    }
                    
                    # Add type-specific details for failures
                    if query_type == "BETWEEN":
                        range_start, range_end = query_values
                        query_detail.update({
                            'range_start': range_start,
                            'range_end': range_end,
                            'range_description': f"BETWEEN '{range_start}' AND '{range_end}'"
                        })
                    elif query_type == "IN_RANGE":
                        range_start, range_end = query_values
                        query_detail.update({
                            'range_start': range_start,
                            'range_end': range_end,
                            'range_description': f"IN ('{range_start}','{range_end}') - acts as range"
                        })
                    else:  # Fallback
                        query_detail.update({
                            'query_description': str(query_values)
                        })
                    
                    results['query_details'].append(query_detail)
        
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
        """Run the complete evaluation"""
        # Give server time to stabilize after startup
        print("⏰ Allowing server startup stabilization...")
        time.sleep(3)
        
        # Simplified configuration parameters for numeric testing
        configs = [
            (1, 0.025),
            (0, 0.025),
            (2, 0.025),
            (3, 0.025)
        ]
        
        results = []
        
        for variant, approx_fp_rate in configs:
            config_name = f"appx_fp_{approx_fp_rate}"
            granularity = 1000
            
            print(f"\n{'='*60}")
            print(f"🚀 Testing Configuration: {config_name}")
            print(f"   Approx FP Rate: {approx_fp_rate}")
            print(f"   Granularity: {granularity}")
            print(f"{'='*60}")
            
            # Strip dots from config_name for table names
            safe_config_name = config_name.replace('.', '')
            
            # Generate separate nonces for each table to prevent metric pollution
            surf_nonce = str(uuid.uuid4()).replace('-', '')[:8]
            minmax_nonce = str(uuid.uuid4()).replace('-', '')[:8]
            
            surf_table = f"test_surf_{safe_config_name}_{surf_nonce}"
            minmax_table = f"test_minmax_{safe_config_name}_{minmax_nonce}"
            
            print(f"📋 SuRF table: {surf_table}")
            print(f"📋 Minmax table: {minmax_table}")
            
            # Step 1: Delete existing tables
            self.delete_tables_if_exist([surf_table, minmax_table])
            
            # Step 2: Create tables (without indexes)
            surf_success = self.create_surf_table(surf_table, granularity)
            minmax_success = self.create_minmax_table(minmax_table, granularity)

            if not (surf_success and minmax_success):
                print(f"✗ Failed to create tables for config {config_name}")
                continue
            
            # Step 3: Insert test data (1 million rows) - same data for both tables
            self.insert_test_data(surf_table, 1000000)
            self.insert_test_data(minmax_table, 1000000)
            
            # Step 4: Create indexes and measure creation time
            surf_construction_time = self.create_surf_index(surf_table, variant, surf_nonce)
            minmax_construction_time = self.create_minmax_index(minmax_table, approx_fp_rate, minmax_nonce)
            
            # Restart ClickHouse server after data insertion to test persistence
            print("🔄 Restarting ClickHouse server after data insertion...")
            self.restart_clickhouse_server()
            
            # Generate appropriate test queries for each index type
            # SuRF gets IN queries (exact value matching strength)
            surf_test_queries = self.generate_surf_queries(50, surf_nonce)
            # MinMax gets BETWEEN queries (range filtering strength)  
            minmax_test_queries = self.generate_minmax_queries(50, minmax_nonce)
            
            surf_results = self.run_query_performance_test(surf_table, surf_test_queries, 1, surf_nonce)
            minmax_results = self.run_query_performance_test(minmax_table, minmax_test_queries, 1, minmax_nonce)
            
            # Get index sizes
            surf_sizes = self.get_index_sizes(surf_table)
            minmax_sizes = self.get_index_sizes(minmax_table)
            
            # Compile results
            config_results = {
                'config': config_name,
                'approx_fp_rate': approx_fp_rate,
                'granularity': granularity,
                'surf': {
                    'performance': surf_results,
                    'sizes': surf_sizes,
                    'construction_time_seconds': surf_construction_time
                },
                'minmax': {
                    'performance': minmax_results,
                    'sizes': minmax_sizes,
                    'construction_time_seconds': minmax_construction_time
                }
            }
            
            results.append(config_results)
            
            # Print intermediate results
            self.print_config_results(config_results)
            
            # Cleanup tables to save space - DISABLED to keep tables for analysis
            # self.delete_tables_if_exist([surf_table, minmax_table])
        
        # Print final comparison
        self.print_final_results(results)
        
        return results
    
    def print_config_results(self, config_results: Dict):
        """Print results for a single configuration"""
        config = config_results['config']
        surf = config_results['surf']
        minmax = config_results['minmax']
        
        print(f"\n📊 Results for {config}:")
        print(f"{'─'*50}")
        
        # Performance comparison
        print("🚀 Performance Metrics:")
        surf_latency_ms = surf['performance']['avg_execution_time'] * 1000
        minmax_latency_ms = minmax['performance']['avg_execution_time'] * 1000
        
        print(f"  SuRF   - Latency: {surf_latency_ms:.2f}ms, "
              f"Throughput: {surf['performance']['throughput_qps']:.1f} QPS, "
              f"Avg Granules: {surf['performance']['avg_granules_examined']:.1f}")
        print(f"  Minmax  - Latency: {minmax_latency_ms:.2f}ms, "
              f"Throughput: {minmax['performance']['throughput_qps']:.1f} QPS, "
              f"Avg Granules: {minmax['performance']['avg_granules_examined']:.1f}")
        
        # Granule efficiency comparison
        print("\n🎯 Granule Efficiency:")
        surf_total_granules = surf['performance'].get('total_granules_examined', 0)
        minmax_total_granules = minmax['performance'].get('total_granules_examined', 0)
        surf_excessive = surf['performance'].get('total_excessive_granules', 0)
        minmax_excessive = minmax['performance'].get('total_excessive_granules', 0)
        
        print(f"  SuRF   - FP Rate: {surf['performance']['false_positive_rate']:.4f} ({surf_excessive}/{surf_total_granules} excessive/total)")
        print(f"  Minmax  - FP Rate: {minmax['performance']['false_positive_rate']:.4f} ({minmax_excessive}/{minmax_total_granules} excessive/total)")
        
        # Filtering marks comparison  
        print("\n⚡ Index Filtering Performance:")
        surf_filtering_avg = surf['performance'].get('avg_filtering_marks_per_query', 0)
        minmaxing_avg = minmax['performance'].get('avg_filtering_marks_per_query', 0)
        print(f"  SuRF   - Avg filtering time: {surf_filtering_avg:.1f}μs per query")
        print(f"  Minmax  - Avg filtering time: {minmaxing_avg:.1f}μs per query")
        
        # Size comparison
        print("\n💾 Index Sizes:")
        if 'total' in surf['sizes']:
            print(f"  SuRF   - Compressed: {self.format_bytes(surf['sizes']['total']['compressed_bytes'])}, "
                  f"Uncompressed: {self.format_bytes(surf['sizes']['total']['uncompressed_bytes'])}")
        if 'total' in minmax['sizes']:
            print(f"  Minmax  - Compressed: {self.format_bytes(minmax['sizes']['total']['compressed_bytes'])}, "
                  f"Uncompressed: {self.format_bytes(minmax['sizes']['total']['uncompressed_bytes'])}")
    
    def print_final_results(self, all_results: List[Dict]):
        """Print comprehensive final results"""
        print(f"\n{'='*80}")
        print("🏆 FINAL EVALUATION RESULTS")
        print(f"{'='*80}")
        
        # Create summary table header (comprehensive performance metrics + index sizes + filtering marks)
        print(f"{'Config':<20} {'SuRF Lat(ms)':<11} {'Minmax Lat(ms)':<12} {'SuRF QPS':<9} {'Minmax QPS':<10} {'SuRF FP Rate':<11} {'Minmax FP Rate':<12} {'SuRF Gran':<9} {'Minmax Gran':<10} {'SuRF Filt(μs)':<12} {'Minmax Filt(μs)':<14} {'SuRF Comp(KB)':<12} {'SuRF Uncomp(KB)':<14} {'Minmax Comp(KB)':<14} {'Minmax Uncomp(KB)':<16}")
        print("─" * 230)
        
        # Create summary data
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            minmax_perf = result['minmax']['performance']
            surf_sizes = result['surf']['sizes']
            minmax_sizes = result['minmax']['sizes']
            
            # Convert latency from seconds to milliseconds
            surf_latency_ms = surf_perf['avg_execution_time'] * 1000
            minmax_latency_ms = minmax_perf['avg_execution_time'] * 1000
            
            # Get index sizes in KB (both compressed and uncompressed)
            surf_comp_kb = surf_sizes.get('total', {}).get('compressed_bytes', 0) / 1024
            surf_uncomp_kb = surf_sizes.get('total', {}).get('uncompressed_bytes', 0) / 1024
            minmax_comp_kb = minmax_sizes.get('total', {}).get('compressed_bytes', 0) / 1024
            minmax_uncomp_kb = minmax_sizes.get('total', {}).get('uncompressed_bytes', 0) / 1024
            
            # Get filtering marks average per query
            surf_filtering_avg = surf_perf.get('avg_filtering_marks_per_query', 0)
            minmaxing_avg = minmax_perf.get('avg_filtering_marks_per_query', 0)
            
            print(f"{config:<20} "
                  f"{surf_latency_ms:<11.2f} "
                  f"{minmax_latency_ms:<12.2f} "
                  f"{surf_perf['throughput_qps']:<9.1f} "
                  f"{minmax_perf['throughput_qps']:<10.1f} "
                  f"{surf_perf['false_positive_rate']:<11.4f} "
                  f"{minmax_perf['false_positive_rate']:<12.4f} "
                  f"{surf_perf['avg_granules_examined']:<9.1f} "
                  f"{minmax_perf['avg_granules_examined']:<10.1f} "
                  f"{surf_filtering_avg:<12.1f} "
                  f"{minmaxing_avg:<14.1f} "
                  f"{surf_comp_kb:<12.1f} "
                  f"{surf_uncomp_kb:<14.1f} "
                  f"{minmax_comp_kb:<14.1f} "
                  f"{minmax_uncomp_kb:<16.1f}")
        
        # Print detailed false positive analysis
        print(f"\n📈 False Positive Ratio Analysis:")
        print(f"{'Config':<20} {'SuRF Avg FP Ratio':<16} {'Minmax Avg FP Ratio':<18} {'SuRF Max FP Ratio':<16} {'Minmax Max FP Ratio':<18}")
        print("─" * 90)
        
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            minmax_perf = result['minmax']['performance']
            
            print(f"{config:<20} "
                  f"{surf_perf['avg_false_positive_ratio']:<16.3f} "
                  f"{minmax_perf['avg_false_positive_ratio']:<18.3f} "
                  f"{surf_perf['max_false_positive_ratio']:<16.3f} "
                  f"{minmax_perf['max_false_positive_ratio']:<18.3f}")
        
        # Print excessive granule analysis
        print(f"\n🔍 Excessive Granule Analysis:")
        print(f"{'Config':<20} {'SuRF Total Excessive':<19} {'Minmax Total Excessive':<21} {'SuRF Avg Excessive':<17} {'Minmax Avg Excessive':<19}")
        print("─" * 100)
        
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            minmax_perf = result['minmax']['performance']
            
            print(f"{config:<20} "
                  f"{surf_perf['total_excessive_granules']:<19} "
                  f"{minmax_perf['total_excessive_granules']:<21} "
                  f"{surf_perf['avg_excessive_granules']:<17.2f} "
                  f"{minmax_perf['avg_excessive_granules']:<19.2f}")
        
        # Print index construction time analysis
        print(f"\n⏱️ Index Construction Time Analysis:")
        print(f"{'Config':<20} {'SuRF Construction (s)':<20} {'Minmax Construction (s)':<22} {'Speedup (Minmax/SuRF)':<20}")
        print("─" * 85)
        
        for result in all_results:
            config = result['config']
            surf_time = result['surf'].get('construction_time_seconds', 0)
            minmax_time = result['minmax'].get('construction_time_seconds', 0)
            speedup = minmax_time / surf_time if surf_time > 0 else 0
            
            print(f"{config:<20} "
                  f"{surf_time:<20.3f} "
                  f"{minmax_time:<22.3f} "
                  f"{speedup:<20.2f}x")

        # Save detailed JSON
        session_id = str(uuid.uuid4()).replace('-', '')[:8]
        json_filename = f"surf_vs_minmax_detailed_{session_id}_{int(time.time())}.json"
        with open(json_filename, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n📄 Detailed results saved to {json_filename}")
        print(f"🎯 Session ID: {session_id}")

def main():
    parser = argparse.ArgumentParser(description='SuRF vs Minmax Filter Performance Evaluation - String Range Queries')
    parser.add_argument('--client-path', default='./build/programs/clickhouse', 
                       help='Path to ClickHouse client binary')
    
    args = parser.parse_args()
    
    print("🎯 Starting SuRF vs Minmax Filter Evaluation (String Range Queries)")
    print(f"   Using ClickHouse client: {args.client_path}")
    print("   Test data: 1M words from words.txt (random insertion order)")
    print("   SuRF queries: IN clauses (word IN ('start','end')) - acts as range query for SuRF")
    print("   MinMax queries: BETWEEN clauses (word BETWEEN 'start' AND 'end') - standard range filtering")
    print("   Index granularity: 1000 (fixed)")
    
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
