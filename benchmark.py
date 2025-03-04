import time
import statistics
from roracle.ror_matcher import find_ror_records

def run_benchmark(n_iterations=5):
    print(f"Running benchmark for find_ror_records ({n_iterations} iterations per test)...")
    
    # Test cases
    test_cases = [
        "Harvard Medical School, MA",
        "University of California, Berkeley",
        "Harvard Medical School; Harvard University",
        "Department of Physics, MIT, Cambridge, MA",
        "Max Planck Institute for Astrophysics, Garching, Germany",
        "Department of Computer Science, Stanford University, USA",
    ]
    
    results = {}
    
    for test_case in test_cases:
        print(f"\nTesting: {test_case}")
        times = []
        records = []
        
        for i in range(n_iterations):
            start_time = time.time()
            result = find_ror_records(test_case)
            end_time = time.time()
            
            execution_time = end_time - start_time
            times.append(execution_time)
            records.append(len(result))
            
            print(f"  Iteration {i+1}: {execution_time:.6f} seconds, found {len(result)} records")
            
        avg_time = statistics.mean(times)
        median_time = statistics.median(times)
        min_time = min(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0
        
        results[test_case] = {
            "avg_time": avg_time,
            "median_time": median_time,
            "min_time": min_time,
            "std_dev": std_dev,
            "records": records[0]  # Use the first iteration's record count
        }
        
        print(f"  Average: {avg_time:.6f} seconds")
        print(f"  Median: {median_time:.6f} seconds")
        print(f"  Min: {min_time:.6f} seconds")
        print(f"  Std Dev: {std_dev:.6f} seconds")
        print(f"  Found {records[0]} records: {[r.id for r in result]}")
        
    print("\nSummary:")
    for test_case, stats in results.items():
        print(f"{test_case}: {stats['avg_time']:.6f}s avg, {stats['records']} records")
        
if __name__ == "__main__":
    run_benchmark()
