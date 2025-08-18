#!/usr/bin/env python3
"""
SuRF vs Bloom Filter Performance Evaluation Script (No External Dependencies)

This script compares the performance of SuRF (ngramsf_v1, tokensf_v1) 
vs Bloom (ngrambf_v1, tokenbf_v1) filters across different configurations.
Uses only standard Python libraries and ClickHouse client binary.
"""

import subprocess
import random
import string
import time
import json
import os
import re
import argparse
import uuid
from typing import List, Dict, Tuple

class ClickHouseIndexEvaluator:
    def __init__(self, clickhouse_client_path='./build/programs/clickhouse'):
        """Initialize with ClickHouse client path"""
        self.client_path = clickhouse_client_path
        self.nonce = str(uuid.uuid4()).replace('-', '')[:8]  # Generate 8-character nonce
        print(f"🎯 Evaluation nonce: {self.nonce}")
        
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
    
    def delete_tables_if_exist(self, table_names: List[str]):
        """Delete tables if they exist"""
        for table_name in table_names:
            query = f"DROP TABLE IF EXISTS {table_name}"
            result, success = self.execute_query(query)
            if success:
                print(f"✓ Dropped table {table_name}")
            else:
                print(f"✗ Error dropping table {table_name}")
    
    def create_surf_table(self, table_name: str, filter_size: int, num_hash: int, granularity: int) -> bool:
        """Create table with SuRF indexes (ngramsf_v1, tokensf_v1)"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            id UInt64,
            content String,
            INDEX idx_ngrams content TYPE ngramsf_v1(4, {filter_size}, {num_hash}, 1) GRANULARITY 1,
            INDEX idx_tokens content TYPE tokensf_v1({filter_size}, {num_hash}, 1) GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS index_granularity = 8192
        """
        
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created SuRF table {table_name}")
            return True
        else:
            print(f"✗ Error creating SuRF table {table_name}: {result}")
            return False
    
    def create_bloom_table(self, table_name: str, filter_size: int, num_hash: int, granularity: int) -> bool:
        """Create table with Bloom indexes (ngrambf_v1, tokenbf_v1)"""
        create_sql = f"""
        CREATE TABLE {table_name} (
            id UInt64,
            content String,
            INDEX idx_ngrams content TYPE ngrambf_v1(4, {filter_size}, {num_hash}, 0) GRANULARITY 1,
            INDEX idx_tokens content TYPE tokenbf_v1({filter_size}, {num_hash}, 0) GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS index_granularity = 8192
        """
        
        result, success = self.execute_query(create_sql)
        if success:
            print(f"✓ Created Bloom table {table_name}")
            return True
        else:
            print(f"✗ Error creating Bloom table {table_name}: {result}")
            return False
    
    def insert_test_data(self, table_name: str, num_rows: int = 100000):
        """Insert data from sentences.txt file"""
        print(f"🔄 Inserting data from sentences.txt into {table_name}...")
        
        # Use ClickHouse SQL to insert from sentences.txt file
        insert_query = f"""
        INSERT INTO {table_name} 
        SELECT 
            rowNumberInAllBlocks() as id,
            line as content
        FROM file('sentences.txt', 'LineAsString', 'line String')
        LIMIT {num_rows}
        """
        
        result, success = self.execute_query(insert_query)
        
        if success:
            # Get actual row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result, count_success = self.execute_query(count_query)
            
            if count_success:
                actual_rows = int(count_result.strip()) if count_result.strip() else 0
                print(f"✓ Inserted {actual_rows} rows from sentences.txt into {table_name}")
            else:
                print(f"✓ Inserted data from sentences.txt into {table_name}")
        else:
            print(f"✗ Error inserting data into {table_name}: {result}")
            # Fallback: try alternative path
            fallback_query = f"""
            INSERT INTO {table_name} 
            SELECT 
                rowNumberInAllBlocks() as id,
                line as content
            FROM file('tests/queries/0_stateless/data/sentences.txt', 'LineAsString')
            LIMIT {num_rows}
            """
            
            fallback_result, fallback_success = self.execute_query(fallback_query)
            if fallback_success:
                print(f"✓ Inserted data from tests/queries/0_stateless/data/sentences.txt into {table_name}")
            else:
                print(f"✗ Error with fallback path: {fallback_result}")
                print("Note: Make sure sentences.txt exists in data/ or tests/queries/0_stateless/data/ directory")
    
    def generate_random_token(self, min_len: int = 3, max_len: int = 6) -> str:
        """Generate a random token of specified length"""
        length = random.randint(min_len, max_len)
        return ''.join(random.choices(string.ascii_lowercase, k=length))
    
    def generate_test_queries(self, num_queries: int = 50) -> List[str]:
        """Generate random test queries"""
        queries = []
        for _ in range(num_queries):
            token = self.generate_random_token()
            # Include nonce in query as comment for filtering in query_log
            queries.append(f"SELECT COUNT(*) FROM {{table}} WHERE hasToken(content, '{token}') /* nonce:{self.nonce} */")
        return queries
    
    def run_query_performance_test(self, table_name: str, queries: List[str], iterations: int = 1) -> Dict:
        """Run performance test on queries and collect metrics"""
        results = {
            'table_name': table_name,
            'total_queries': len(queries) * iterations,
            'execution_times': [],
            'index_usage': {'idx_ngrams': [], 'idx_tokens': []},
            'rows_examined': [],
            'false_positives': 0,
            'nonce': self.nonce
        }
        
        print(f"🔄 Running {len(queries)} queries {iterations} times on {table_name}...")
        
        # Record start time for this test batch
        batch_start_time = time.time()
        
        for iteration in range(iterations):
            for i, query_template in enumerate(queries):
                query = query_template.format(table=table_name)
                
                # Run EXPLAIN to get index usage
                explain_query = f"EXPLAIN indexes = 1 {query}"
                
                # Execute actual query (timing will be retrieved from query_log)
                result_output, success = self.execute_query(query)
                
                if success:
                    # Get explain results
                    explain_output, explain_success = self.execute_query(explain_query)
                    
                    if explain_success:
                        # Parse index usage from explain
                        ngram_usage = self.parse_index_usage(explain_output, 'idx_ngrams')
                        token_usage = self.parse_index_usage(explain_output, 'idx_tokens')
                        
                        results['index_usage']['idx_ngrams'].append(ngram_usage)
                        results['index_usage']['idx_tokens'].append(token_usage)
                    
                    # Count rows examined (false positives)
                    try:
                        rows_count = int(result_output.strip()) if result_output.strip() else 0
                        results['rows_examined'].append(rows_count)
                        
                        if rows_count > 0:
                            results['false_positives'] += 1
                    except ValueError:
                        results['rows_examined'].append(0)
                        
                else:
                    print(f"✗ Query failed: {result_output}")
        
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
        results['false_positive_rate'] = results['false_positives'] / results['total_queries']
        
        print(f"✓ Completed performance test for {table_name}")
        return results
    
    def parse_index_usage(self, explain_text: str, index_name: str) -> Dict:
        """Parse index usage statistics from EXPLAIN output"""
        # Look for patterns like "Index `idx_ngrams` has dropped 3/10 granules"
        pattern = rf"Index `{index_name}` has dropped (\d+)/(\d+) granules"
        match = re.search(pattern, explain_text)
        
        if match:
            dropped = int(match.group(1))
            total = int(match.group(2))
            return {
                'dropped_granules': dropped,
                'total_granules': total,
                'efficiency': (total - dropped) / total if total > 0 else 0
            }
        return {'dropped_granules': 0, 'total_granules': 0, 'efficiency': 0}

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
        # Configuration parameters
        configs = [
            (8192, 5),
            (4096, 5),
            (2048, 4),
            (1024, 4),
            (512, 3),
            (256, 2),
            (128, 2)
        ]
        
        results = []
        
        for filter_size, num_hash in configs:
            granularity = filter_size
            config_name = f"size_{filter_size}_hash_{num_hash}"
            
            print(f"\n{'='*60}")
            print(f"🚀 Testing Configuration: {config_name}")
            print(f"   Filter Size: {filter_size} bytes")
            print(f"   Hash Functions: {num_hash}")
            print(f"   Granularity: {granularity}")
            print(f"{'='*60}")
            
            surf_table = f"test_surf_{config_name}_{self.nonce}"
            bloom_table = f"test_bloom_{config_name}_{self.nonce}"
            
            # Step 1: Delete existing tables
            self.delete_tables_if_exist([surf_table, bloom_table])
            
            # Step 2 & 3: Create tables
            surf_success = self.create_surf_table(surf_table, filter_size, num_hash, granularity)
            bloom_success = self.create_bloom_table(bloom_table, filter_size, num_hash, granularity)
            
            if not (surf_success and bloom_success):
                print(f"✗ Failed to create tables for config {config_name}")
                continue
            
            # Insert test data
            self.insert_test_data(surf_table)
            self.insert_test_data(bloom_table)
            
            # Step 4 & 5: Generate and run test queries
            test_queries = self.generate_test_queries(50)  # Reduced from 100
            
            surf_results = self.run_query_performance_test(surf_table, test_queries, 1)  # Reduced from 10
            bloom_results = self.run_query_performance_test(bloom_table, test_queries, 1)  # Reduced from 10
            
            # Step 6 & 7: Get index sizes
            surf_sizes = self.get_index_sizes(surf_table)
            bloom_sizes = self.get_index_sizes(bloom_table)
            
            # Compile results
            config_results = {
                'config': config_name,
                'filter_size': filter_size,
                'num_hash': num_hash,
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
        
        # Step 8: Print final comparison
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
        print(f"  SuRF   - Avg: {surf['performance']['avg_execution_time']:.4f}s, "
              f"Throughput: {surf['performance']['throughput_qps']:.2f} QPS, "
              f"FP Rate: {surf['performance']['false_positive_rate']:.3f}")
        print(f"  Bloom  - Avg: {bloom['performance']['avg_execution_time']:.4f}s, "
              f"Throughput: {bloom['performance']['throughput_qps']:.2f} QPS, "
              f"FP Rate: {bloom['performance']['false_positive_rate']:.3f}")
        
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
        
        # Create summary table header
        print(f"{'Config':<20} {'Filter Size':<12} {'Hash':<5} {'SuRF QPS':<10} {'Bloom QPS':<11} {'SuRF FP':<8} {'Bloom FP':<9} {'SuRF Size':<10} {'Bloom Size':<11}")
        print("─" * 100)
        
        # Create summary data
        for result in all_results:
            config = result['config']
            surf_perf = result['surf']['performance']
            bloom_perf = result['bloom']['performance']
            surf_size = result['surf']['sizes'].get('total', {})
            bloom_size = result['bloom']['sizes'].get('total', {})
            
            print(f"{config:<20} "
                  f"{result['filter_size']:<12} "
                  f"{result['num_hash']:<5} "
                  f"{surf_perf['throughput_qps']:<10.2f} "
                  f"{bloom_perf['throughput_qps']:<11.2f} "
                  f"{surf_perf['false_positive_rate']:<8.3f} "
                  f"{bloom_perf['false_positive_rate']:<9.3f} "
                  f"{surf_size.get('compressed_bytes', 0) / 1024 / 1024:<10.2f} "
                  f"{bloom_size.get('compressed_bytes', 0) / 1024 / 1024:<11.2f}")
        
        # Save detailed JSON
        json_filename = f"surf_vs_bloom_detailed_{self.nonce}_{int(time.time())}.json"
        with open(json_filename, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n📄 Detailed results saved to {json_filename}")
        print(f"🎯 Evaluation nonce: {self.nonce}")

def main():
    parser = argparse.ArgumentParser(description='SuRF vs Bloom Filter Performance Evaluation')
    parser.add_argument('--client-path', default='./build/programs/clickhouse', 
                       help='Path to ClickHouse client binary')
    
    args = parser.parse_args()
    
    print("🎯 Starting SuRF vs Bloom Filter Evaluation")
    print(f"   Using ClickHouse client: {args.client_path}")
    
    try:
        evaluator = ClickHouseIndexEvaluator(args.client_path)
        print(f"   Evaluation ID: {evaluator.nonce}")
        results = evaluator.run_evaluation()
        print("\n✅ Evaluation completed successfully!")
        print(f"🎯 Final nonce: {evaluator.nonce}")
        
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
