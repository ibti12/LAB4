from pyspark import SparkContext, SparkConf, StorageLevel
import time

conf = SparkConf().setAppName("Lab2-Persistence").setMaster("local[*]")

sc = SparkContext(conf=conf)

print("=" * 80)
print("LAB 2: RDD PERSISTENCE AND CACHING STRATEGIES")
print("=" * 80)

# =====================================================
# PART 1: Understanding the Problem - No Caching
# =====================================================
print("\n[PART 1] Baseline - No Caching\n")

# Create expensive computation
def expensive_transformation(x):
    """Simulate expensive computation"""
    result = x
    for _ in range(1000):
        result = (result * 1.1 + 0.5) / 1.05
    return result

# Large dataset
print("Creating large dataset...")
large_data = sc.parallelize(range(1, 100001), numSlices=8)  # 100k elements

# Apply expensive transformation
print("Applying expensive transformation...")
processed_rdd = large_data.map(expensive_transformation)

# Multiple actions on same RDD (without caching)
print("\nExecuting multiple actions WITHOUT caching:")

start = time.time()
count1 = processed_rdd.count()
time1 = time.time() - start
print(f"  Action 1 (count):  {time1:.3f}s - Count: {count1}")

start = time.time()
sum_result = processed_rdd.reduce(lambda a, b: a + b)
time2 = time.time() - start
print(f"  Action 2 (sum):    {time2:.3f}s - Sum: {sum_result:.2f}")

start = time.time()
max_val = processed_rdd.max()
time3 = time.time() - start
print(f"  Action 3 (max):    {time3:.3f}s - Max: {max_val:.2f}")

total_time_no_cache = time1 + time2 + time3
print(f"\n  Total time: {total_time_no_cache:.3f}s")
print("  ⚠️  Each action recomputes the entire RDD from scratch!")

# =====================================================
# PART 2: Using cache() - Memory Storage
# =====================================================
print("\n[PART 2] Using cache() - Memory Storage\n")

# Recreate RDD and apply caching
large_data_cached = sc.parallelize(range(1, 100001), numSlices=8)
processed_cached = large_data_cached.map(expensive_transformation)

# Cache in memory
print("Calling cache() on RDD...")
processed_cached.cache()

print("\nExecuting multiple actions WITH cache():")

start = time.time()
count1 = processed_cached.count()  # Triggers computation + caching
time1 = time.time() - start
print(f"  Action 1 (count):  {time1:.3f}s - Count: {count1} [COMPUTED + CACHED]")

start = time.time()
sum_result = processed_cached.reduce(lambda a, b: a + b)  # Uses cache
time2 = time.time() - start
print(f"  Action 2 (sum):    {time2:.3f}s - Sum: {sum_result:.2f} [FROM CACHE]")

start = time.time()
max_val = processed_cached.max()  # Uses cache
time3 = time.time() - start
print(f"  Action 3 (max):    {time3:.3f}s - Max: {max_val:.2f} [FROM CACHE]")

total_time_cached = time1 + time2 + time3
print(f"\n  Total time: {total_time_cached:.3f}s")

improvement = ((total_time_no_cache - total_time_cached) / total_time_no_cache) * 100
print(f"  ⚡ Performance improvement: {improvement:.1f}%")

# =====================================================
# PART 3: Different Storage Levels
# =====================================================
print("\n[PART 3] Comparing Different Storage Levels\n")

storage_levels = [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
    ("DISK_ONLY", StorageLevel.DISK_ONLY)
]

print(f"{'Storage Level':<20} {'First Action':<15} {'Second Action':<15} {'Total':<10}")
print("-" * 65)

comparison_results = []

for name, level in storage_levels:
    # Create fresh RDD
    test_data = sc.parallelize(range(1, 100001), numSlices=8)
    test_rdd = test_data.map(expensive_transformation)
    
    # Persist with specific storage level
    test_rdd.persist(level)
    
    # First action (computation + storage)
    start = time.time()
    test_rdd.count()
    time_first = time.time() - start
    
    # Second action (from storage)
    start = time.time()
    test_rdd.reduce(lambda a, b: a + b)
    time_second = time.time() - start
    
    total = time_first + time_second
    print(f"{name:<20} {time_first:<15.3f} {time_second:<15.3f} {total:<10.3f}")
    
    comparison_results.append((name, time_first, time_second, total))
    
    # Cleanup
    test_rdd.unpersist()

# Find best performance
best = min(comparison_results, key=lambda x: x[3])
print(f"\n⚡ Best performance: {best[0]} (Total: {best[3]:.3f}s)")

# =====================================================
# PART 4: Storage Level Characteristics
# =====================================================
print("\n[PART 4] Storage Level Characteristics\n")

print("""
┌─────────────────────┬──────────┬──────────┬─────────────┬──────────────┐
│ Storage Level       │ Memory   │ Disk     │ Serialized  │ Recompute    │
├─────────────────────┼──────────┼──────────┼─────────────┼──────────────┤
│ MEMORY_ONLY         │ Yes      │ No       │ No          │ On eviction  │
│ MEMORY_AND_DISK     │ Yes      │ Spill    │ No          │ No           │
│ MEMORY_ONLY_SER     │ Yes      │ No       │ Yes         │ On eviction  │
│ MEMORY_AND_DISK_SER │ Yes      │ Spill    │ Yes         │ No           │
│ DISK_ONLY           │ No       │ Yes      │ Yes         │ No           │
│ OFF_HEAP            │ Off-heap │ No       │ Yes         │ On eviction  │
└─────────────────────┴──────────┴──────────┴─────────────┴──────────────┘

Key Observations:
""")

for name, time_first, time_second, total in comparison_results:
    speedup = time_first / time_second if time_second > 0 else 0
    print(f"\n{name}:")
    print(f"  • First action:  {time_first:.3f}s (compute + store)")
    print(f"  • Second action: {time_second:.3f}s (retrieve)")
    print(f"  • Speedup factor: {speedup:.1f}x on cached actions")

# =====================================================
# PART 5: When to Use Each Storage Level
# =====================================================
print("\n[PART 5] When to Use Each Storage Level\n")

print("""
MEMORY_ONLY (Default for cache()):
  ✓ Use when: Dataset fits in memory
  ✓ Best for: Fast access, iterative algorithms
  ✗ Risk: Data loss if memory pressure
  📊 Example: Small to medium datasets, ML training

MEMORY_AND_DISK:
  ✓ Use when: Dataset might not fit in memory
  ✓ Best for: Production jobs, critical data
  ✗ Tradeoff: Slower disk access vs recomputation
  📊 Example: Large datasets, long computations

MEMORY_ONLY_SER (Serialized):
  ✓ Use when: Memory is limited but CPU is available
  ✓ Best for: Space-efficient storage
  ✗ Tradeoff: CPU overhead for serialization/deserialization
  📊 Example: Large objects, memory-constrained environments

MEMORY_AND_DISK_SER:
  ✓ Use when: Need space efficiency + reliability
  ✓ Best for: Large production jobs
  ✗ Tradeoff: CPU overhead + potential disk I/O
  📊 Example: Enterprise ETL jobs

DISK_ONLY:
  ✓ Use when: Data is too large for memory
  ✓ Best for: Archival, very large intermediate results
  ✗ Tradeoff: Slowest access, but no memory used
  📊 Example: Data preparation, large shuffles

OFF_HEAP:
  ✓ Use when: Need precise memory management
  ✓ Best for: Large cached data, reduce GC pressure
  ✗ Tradeoff: Requires configuration, serialization overhead
  📊 Example: Long-running applications, large caches
""")

# =====================================================
# PART 6: Cache vs Persist - Practical Comparison
# =====================================================
print("\n[PART 6] cache() vs persist() - What's the Difference?\n")

print("""
cache() vs persist():

cache():
  • Shorthand for persist(StorageLevel.MEMORY_ONLY)
  • Simple API for common use case
  • Example: rdd.cache()

persist(storageLevel):
  • Explicit storage level control
  • More flexibility for optimization
  • Example: rdd.persist(StorageLevel.MEMORY_AND_DISK)

RECOMMENDATION:
  • Use cache() for quick prototyping and small datasets
  • Use persist() with specific level for production code
  • Always consider memory availability and access patterns
""")

# =====================================================
# PART 7: Real-World Scenarios
# =====================================================
print("\n[PART 7] Real-World Scenarios and Recommendations\n")

scenarios = [
    {
        "name": "Iterative Machine Learning",
        "description": "Training model with multiple passes over data",
        "recommendation": "MEMORY_ONLY or MEMORY_AND_DISK",
        "reason": "Multiple iterations benefit from fast memory access"
    },
    {
        "name": "ETL Pipeline with Multiple Outputs",
        "description": "Process data once, write to multiple destinations",
        "recommendation": "MEMORY_AND_DISK",
        "reason": "Ensures data persists through multiple actions"
    },
    {
        "name": "Exploratory Data Analysis",
        "description": "Interactive queries on same dataset",
        "recommendation": "MEMORY_ONLY or MEMORY_ONLY_SER",
        "reason": "Fast interactive response times"
    },
    {
        "name": "Large-Scale Log Processing",
        "description": "Process TB-scale logs with aggregations",
        "recommendation": "MEMORY_AND_DISK_SER or DISK_ONLY",
        "reason": "Data too large for memory, need fault tolerance"
    },
    {
        "name": "Stream Processing with Windowing",
        "description": "Maintain windows of recent data",
        "recommendation": "MEMORY_AND_DISK",
        "reason": "Balance speed and reliability for real-time"
    }
]

for i, scenario in enumerate(scenarios, 1):
    print(f"\nScenario {i}: {scenario['name']}")
    print(f"  Description: {scenario['description']}")
    print(f"  ✓ Recommended: {scenario['recommendation']}")
    print(f"  💡 Reason: {scenario['reason']}")

# =====================================================
# PART 8: Best Practices and Pitfalls
# =====================================================
print("\n[PART 8] Best Practices and Common Pitfalls\n")

print("""
BEST PRACTICES:
✓ Cache only RDDs that are reused multiple times
✓ Call cache() before the first action (it's lazy)
✓ Unpersist RDDs when done to free memory
✓ Monitor memory usage in Spark UI
✓ Use appropriate storage level for your use case
✓ Consider serialization cost vs memory savings

COMMON PITFALLS:
✗ Caching everything (memory waste)
✗ Caching RDDs used only once (no benefit)
✗ Not unpersisting unused cached RDDs
✗ Using MEMORY_ONLY for large datasets (eviction thrashing)
✗ Ignoring Spark UI memory metrics
✗ Over-serialization (CPU overhead)

MEMORY MANAGEMENT TIPS:
• spark.memory.fraction (default 0.6): Execution + storage
• spark.memory.storageFraction (default 0.5): Storage within memory
• Monitor "Storage" tab in Spark UI
• Use rdd.getStorageLevel() to check current level
• Check "Blocked" memory in UI for evictions
""")

# =====================================================
# PART 9: Practical Exercise
# =====================================================
print("\n[PART 9] Practical Exercise - Finding Optimal Strategy\n")

# Simulate a multi-stage pipeline
print("Simulating multi-stage data pipeline...\n")

# Stage 1: Load and clean
raw_data = sc.parallelize(range(1, 50001), numSlices=8)
cleaned = raw_data.map(lambda x: x * 2).filter(lambda x: x % 3 != 0)

# Stage 2: Enrich (expensive)
enriched = cleaned.map(expensive_transformation)

# Stage 3: Aggregate (multiple actions)
print("Testing different caching strategies:")

strategies = [
    ("No caching", None),
    ("Cache cleaned", "cleaned"),
    ("Cache enriched", "enriched"),
    ("Cache both", "both")
]

for strategy_name, cache_point in strategies:
    # Recreate pipeline
    raw = sc.parallelize(range(1, 50001), numSlices=8)
    clean = raw.map(lambda x: x * 2).filter(lambda x: x % 3 != 0)
    enrich = clean.map(expensive_transformation)
    
    # Apply caching strategy
    if cache_point == "cleaned":
        clean.cache()
    elif cache_point == "enriched":
        enrich.cache()
    elif cache_point == "both":
        clean.cache()
        enrich.cache()
    
    # Measure performance
    start = time.time()
    count = enrich.count()
    sum_val = enrich.reduce(lambda a, b: a + b)
    max_val = enrich.max()
    total_time = time.time() - start
    
    print(f"  {strategy_name:20s}: {total_time:.3f}s")
    
    # Cleanup
    clean.unpersist()
    enrich.unpersist()

print("\n💡 Observation: Cache at the point where computation becomes expensive")
print("   and the data will be reused multiple times.")

# =====================================================
# SUMMARY
# =====================================================
print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)
print(f"""
PERFORMANCE SUMMARY:
• Without caching: {total_time_no_cache:.3f}s (baseline)
• With caching:    {total_time_cached:.3f}s
• Improvement:     {improvement:.1f}%

WHEN TO CACHE:
✓ RDD is used 2+ times in your application
✓ Computation is expensive (complex transformations)
✓ Dataset fits in available memory (or use MEMORY_AND_DISK)
✓ Iterative algorithms (ML, graph processing)

WHEN NOT TO CACHE:
✗ RDD is used only once
✗ Data is too large and causes memory pressure
✗ Simple transformations (faster to recompute)
✗ Streaming with non-overlapping windows

STORAGE LEVEL DECISION TREE:
1. Will RDD be reused? → No: Don't cache
2. Dataset size vs available memory?
   → Fits easily: MEMORY_ONLY
   → Might not fit: MEMORY_AND_DISK
   → Definitely too large: DISK_ONLY or recompute
3. Is serialization worthwhile?
   → Large objects: Yes, use *_SER variant
   → Small objects: No, overhead not worth it

MONITORING:
• Spark UI → Storage tab: See cached RDDs
• Check RDD size and percentage cached
• Monitor memory usage and evictions
• Use rdd.toDebugString() to see lineage
""")

# Cleanup
processed_cached.unpersist()

sc.stop()