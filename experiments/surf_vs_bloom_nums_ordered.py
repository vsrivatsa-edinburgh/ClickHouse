#!/usr/bin/env python3
"""
SuRF vs Bloom Filter Performance Evaluation Script - Numeric Point Queries

This script compares the performance of SuRF vs Bloom filters for numeric point queries.
Uses simple 0-1M numeric data with ID-based equality queries.
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
        self.nonce = str(uuid.uuid4()).replace('-', '')[:8]  # Generate 8-character nonce
        print(f"🎯 Evaluation nonce: {self.nonce}")
        
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

    def create_surf_table(self, table_name: str, approx_fp_rate: float, granularity: int) -> bool:
        """Create table with SuRF indexes for numeric data"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            id Int64,
            INDEX idx_id id TYPE surf_filter({approx_fp_rate}) GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created SuRF table {table_name}")
            return True
        else:
            print(f"✗ Error creating SuRF table {table_name}: {result}")
            print(create_sql)
            return False

    def create_bloom_table(self, table_name: str, approx_fp_rate: float, granularity: int) -> bool:
        """Create table with Bloom indexes for numeric data"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            id Int64,
            INDEX idx_id id TYPE bloom_filter({approx_fp_rate}) GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY ()
        SETTINGS index_granularity = {granularity}
        """
        
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created Bloom table {table_name}")
            return True
        else:
            print(f"✗ Error creating Bloom table {table_name}: {result}")
            print(create_sql)
            return False
    
    def insert_test_data(self, table_name: str, num_rows: int = 1000000):
        """Insert numeric data maintaining order for effective data skipping index usage"""
        print(f"🔄 Inserting {num_rows} numeric rows into {table_name}...")
        
        # Add delay before insertion
        print("⏳ Delay before insertion...")
        time.sleep(2)
        
        insert_query = f"""
        INSERT INTO {table_name} 
        SELECT 
            number as id
        FROM numbers(1, {num_rows})
        """
        
        result, success = self.execute_query(insert_query)
        
        if not success:
            print(f"✗ Error inserting numbers: {result}")
            return
        
        # Add delay after insertion
        print("⏳ Delay after insertion...")
        time.sleep(3)
        
        # Get actual row count
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count_result, count_success = self.execute_query(count_query)
        
        if count_success:
            actual_rows = int(count_result.strip()) if count_result.strip() else 0
            print(f"✓ Inserted {actual_rows} numeric rows into {table_name}")
        else:
            print(f"✓ Inserted numeric data into {table_name}")
        
        print("💥 Crashing server after insertion to test persistence...")
        time.sleep(1)
    
    def generate_test_queries(self, num_queries: int = 50) -> List[Tuple[str, int, bool]]:
        """Generate random point queries for ID equality with metadata"""
        queries = []
        for _ in range(num_queries):
            # Generate random ID between 0 and 999,999 (existing) or higher (non-existing)
            if random.random() < 0.5:  # 70% existing IDs
                target_id = random.randint(0, 999999)
                should_exist = True
            else:  # 50% non-existing IDs
                target_id = random.randint(1000000, 1999999)
                should_exist = False
            
            query = f"SELECT COUNT(*) FROM {{table}} WHERE id = {target_id} SETTINGS force_data_skipping_indices='idx_id' /* nonce:{self.nonce} */"
            queries.append((query, target_id, should_exist))
        return queries
    
    def run_query_performance_test(self, table_name: str, queries: List[Tuple[str, int, bool]], iterations: int = 1) -> Dict:
        """Run performance test on queries and collect metrics"""
        results = {
            'table_name': table_name,
            'total_queries': len(queries) * iterations,
            'execution_times': [],
            'index_usage': {'idx_id': []},
            'granules_examined': [],
            'query_details': [],  # Store detailed results per query
            'nonce': self.nonce
        }
        
        print(f"🔄 Running {len(queries)} queries {iterations} times on {table_name}...")
        
        # Get baseline filtering marks metric before starting
        baseline_filtering_marks = self.get_filtering_marks_metric()
        print(f"📊 Baseline FilteringMarksWithSecondaryKeysMicroseconds: {baseline_filtering_marks}")
        
        # Record start time for this test batch
        batch_start_time = time.time()
        
        for iteration in range(iterations):
            for i, (query_template, target_id, should_exist) in enumerate(queries):
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
                    
                    if explain_success:
                        # Parse index usage from explain
                        id_usage = self.parse_index_usage(explain_output, 'idx_id')
                        results['index_usage']['idx_id'].append(id_usage)
                        
                        # Calculate granules examined vs expected
                        total_granules = id_usage.get('total_granules', 0)
                        scanned_granules = id_usage.get('scanned_granules', 0)  # Actually scanned granules
                        
                        print(f"    Query ID={target_id}, should_exist={should_exist}")
                        print(f"    Total granules: {total_granules}, Scanned: {scanned_granules}")

                        # Calculate excessive granules and false positive ratio
                        if should_exist:
                            # For existing IDs, we expect exactly 1 granule to be examined
                            expected_granules = 1
                            excessive_granules = max(0, scanned_granules - expected_granules)
                        else:
                            # For non-existing IDs, we expect 0 granules to be examined
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
                    
                    # Store detailed query information (no binary false positive tracking)
                    results['query_details'].append({
                        'target_id': target_id,
                        'should_exist': should_exist,
                        'granules_examined': scanned_granules,
                        'excessive_granules': excessive_granules,
                        'false_positive_ratio': false_positive_ratio,
                        'index_usage': id_usage
                    })
                        
                else:
                    print(f"✗ Query failed: {result_output}")
                    results['granules_examined'].append(0)
                    results['query_details'].append({
                        'target_id': target_id,
                        'should_exist': should_exist,
                        'granules_examined': 0,
                        'excessive_granules': 0,
                        'false_positive_ratio': 0.0,
                        'index_usage': {}
                    })
        
        # Wait a moment for query_log to be updated
        time.sleep(2)
        
        # Get execution times from system.query_log
        execution_times = self.get_execution_times_from_query_log(batch_start_time, iterations*len(queries), table_name)
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

    def get_execution_times_from_query_log(self, start_time: float, limit: int, table_name: str) -> List[float]:
        """Get execution times from system.query_log using nonce filtering"""
        # Convert start_time to ClickHouse format
        start_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_time))
        
        query_log_query = f"""
        SELECT query_duration_ms / 1000.0 as execution_time
        FROM system.query_log 
        WHERE query LIKE '%nonce:{self.nonce}%' AND query LIKE '%{table_name}%'
          AND type = 'QueryFinish'
          AND event_time >= '{start_datetime}'
          AND query NOT LIKE '%EXPLAIN%'
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
            (1, 0.01),
            (0, 0.025),
            (2, 0.05)
        ]
        
        results = []
        
        for variant, approx_fp_rate in configs:
            config_name = f"appx_fp_{approx_fp_rate}"
            granularity = 100
            
            print(f"\n{'='*60}")
            print(f"🚀 Testing Configuration: {config_name}")
            print(f"   Approx FP Rate: {approx_fp_rate}")
            print(f"   Granularity: {granularity}")
            print(f"{'='*60}")
            
            # Strip dots from config_name for table names
            safe_config_name = config_name.replace('.', '')
            surf_table = f"test_surf_{safe_config_name}_{self.nonce}"
            bloom_table = f"test_bloom_{safe_config_name}_{self.nonce}"
            
            # Step 1: Delete existing tables
            self.delete_tables_if_exist([surf_table, bloom_table])
            
            # Step 2 & 3: Create tables
            surf_success = self.create_surf_table(surf_table, variant, granularity)
            bloom_success = self.create_bloom_table(bloom_table, approx_fp_rate, granularity)

            if not (surf_success and bloom_success):
                print(f"✗ Failed to create tables for config {config_name}")
                continue
            
            # Insert test data (1 million rows)
            self.insert_test_data(surf_table, 1000000)
            self.insert_test_data(bloom_table, 1000000)
            
            # Restart ClickHouse server after data insertion to test persistence
            print("🔄 Restarting ClickHouse server after data insertion...")
            self.restart_clickhouse_server()
            
            # Generate and run test queries (50 random point queries)
            test_queries = self.generate_test_queries(50)
            
            surf_results = self.run_query_performance_test(surf_table, test_queries, 1)
            bloom_results = self.run_query_performance_test(bloom_table, test_queries, 1)
            
            # Get index sizes
            surf_sizes = self.get_index_sizes(surf_table)
            bloom_sizes = self.get_index_sizes(bloom_table)
            
            # Compile results
            config_results = {
                'config': config_name,
                'approx_fp_rate': approx_fp_rate,
                'granularity': granularity,
                'surf': {
                    'performance': surf_results,
                    'sizes': surf_sizes
                },
                'bloom': {
                    'performance': bloom_results,
                    'sizes': bloom_sizes
                }
            }
            
            results.append(config_results)
            
            # Print intermediate results
            self.print_config_results(config_results)
            
            # Cleanup tables to save space
            self.delete_tables_if_exist([surf_table, bloom_table])
        
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
        
        # Save detailed JSON
        json_filename = f"surf_vs_bloom_detailed_{self.nonce}_{int(time.time())}.json"
        with open(json_filename, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n📄 Detailed results saved to {json_filename}")
        print(f"🎯 Evaluation nonce: {self.nonce}")

def main():
    parser = argparse.ArgumentParser(description='SuRF vs Bloom Filter Performance Evaluation - Numeric Point Queries')
    parser.add_argument('--client-path', default='./build/programs/clickhouse', 
                       help='Path to ClickHouse client binary')
    
    args = parser.parse_args()
    
    print("🎯 Starting SuRF vs Bloom Filter Evaluation (Numeric Point Queries)")
    print(f"   Using ClickHouse client: {args.client_path}")
    print("   Test data: 1M rows (0 to 999,999)")
    print("   Query type: Point queries on ID field")
    print("   Index granularity: 100 (fixed)")
    
    try:
        evaluator = ClickHouseIndexEvaluator(args.client_path)
        print(f"   Evaluation ID: {evaluator.nonce}")
        
        # Start ClickHouse server at the beginning
        print("🚀 Starting ClickHouse server...")
        evaluator.start_clickhouse_server()
        
        results = evaluator.run_evaluation()
        print("\n✅ Evaluation completed successfully!")
        print(f"🎯 Final nonce: {evaluator.nonce}")
        
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
