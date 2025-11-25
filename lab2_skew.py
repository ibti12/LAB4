from pyspark import SparkContext, SparkConf
import time
import random

conf = SparkConf().setAppName("Lab2-DataSkew").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 80)
print("LAB 2: DATA SKEW ANALYSIS AND MITIGATION")
print("=" * 80)

# =====================================================
# PART 1: Demonstrating Data Skew Problem
# =====================================================
print("\n[PART 1] Creating Skewed Dataset\n")

# Create highly skewed data
# 90% of data goes to one key, rest distributed among others
skewed_data = []
for i in range(10000):
    if i < 9000:
        skewed_data.append(("popular_key", i))  # 90% to one key
    else:
        skewed_data.append((f"key_{i % 10}", i))  # 10% distributed

skewed_rdd = sc.parallelize(skewed_data, numSlices=8)

print("Data distribution:")
key_counts = skewed_rdd.countByKey()
for key, count in sorted(key_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {key:20s}: {count:5d} records ({count/100:.1f}%)")

# =====================================================
# PART 2: Observe Skew Impact on Performance
# =====================================================
print("\n[PART 2] Performance Impact of Skew\n")

def expensive_operation(x):
    """Simulate expensive computation"""
    time.sleep(0.001)  # 1ms per record
    return x * 2

# Process skewed data - one partition will be overwhelmed
print("Processing skewed data with groupByKey()...")
start = time.time()
result_skewed = skewed_rdd.groupByKey().mapValues(lambda vals: sum(expensive_operation(v) for v in vals))
result_skewed.count()  # Force computation
time_skewed = time.time() - start

print(f"Time with skewed data: {time_skewed:.2f}s")
print("⚠️  Notice: One partition processes 90% of data - bottleneck!")

# =====================================================
# PART 3: Analyze Partition Distribution
# =====================================================
print("\n[PART 3] Partition Distribution Analysis\n")

def analyze_partition(index, iterator):
    items = list(iterator)
    key_dist = {}
    for key, val in items:
        key_dist[key] = key_dist.get(key, 0) + 1
    yield (index, len(items), key_dist)

partition_analysis = skewed_rdd.mapPartitionsWithIndex(analyze_partition).collect()

print("Partition distribution:")
for part_id, count, keys in partition_analysis:
    print(f"  Partition {part_id}: {count:5d} records")
    if count > 0:
        top_key = max(keys.items(), key=lambda x: x[1])
        print(f"    → Top key: {top_key[0]} ({top_key[1]} records)")

# =====================================================
# PART 4: Solution 1 - Salting Technique
# =====================================================
print("\n[PART 4] Solution 1: Salting Technique\n")

# Add random salt to skewed keys to distribute load
def add_salt(record, num_salts=10):
    key, value = record
    if key == "popular_key":
        salt = random.randint(0, num_salts - 1)
        return (f"{key}_salt_{salt}", value)
    return (key, value)

print("Applying salting to skewed keys...")
salted_rdd = skewed_rdd.map(lambda r: add_salt(r, num_salts=10))

# Check new distribution
print("\nSalted data distribution (top 5):")
salted_counts = salted_rdd.countByKey()
for key, count in sorted(salted_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {key:25s}: {count:5d} records")

# Process salted data
print("\nProcessing salted data...")
start = time.time()
result_salted = salted_rdd.groupByKey().mapValues(lambda vals: sum(expensive_operation(v) for v in vals))
result_salted.count()
time_salted = time.time() - start

print(f"Time with salted data: {time_salted:.2f}s")
improvement = ((time_skewed - time_salted) / time_skewed) * 100
print(f"⚡ Performance improvement: {improvement:.1f}%")

# =====================================================
# PART 5: Solution 2 - Adaptive Partitioning
# =====================================================
print("\n[PART 5] Solution 2: Adaptive Partitioning\n")

# Increase partitions for better distribution
repartitioned_rdd = skewed_rdd.repartition(16)  # Double the partitions

print("Processing with more partitions (16 instead of 8)...")
start = time.time()
result_repart = repartitioned_rdd.groupByKey().mapValues(lambda vals: sum(expensive_operation(v) for v in vals))
result_repart.count()
time_repart = time.time() - start

print(f"Time with repartitioning: {time_repart:.2f}s")
improvement = ((time_skewed - time_repart) / time_skewed) * 100
print(f"⚡ Performance improvement: {improvement:.1f}%")

# =====================================================
# PART 6: Solution 3 - Use reduceByKey Instead
# =====================================================
print("\n[PART 6] Solution 3: Use reduceByKey (Better than groupByKey)\n")

# reduceByKey is more efficient - combines on map side first
print("Using reduceByKey instead of groupByKey...")
start = time.time()
result_reduce = skewed_rdd.mapValues(expensive_operation).reduceByKey(lambda a, b: a + b)
result_reduce.count()
time_reduce = time.time() - start

print(f"Time with reduceByKey: {time_reduce:.2f}s")
improvement = ((time_skewed - time_reduce) / time_skewed) * 100
print(f"⚡ Performance improvement: {improvement:.1f}%")

# =====================================================
# PART 7: Complete Solution - Salting + reduceByKey
# =====================================================
print("\n[PART 7] Complete Solution: Salting + reduceByKey\n")

print("Combining salting with reduceByKey...")
start = time.time()
result_combined = (salted_rdd
    .mapValues(expensive_operation)
    .reduceByKey(lambda a, b: a + b)
    .map(lambda x: (x[0].split("_salt_")[0], x[1]))  # Remove salt
    .reduceByKey(lambda a, b: a + b))  # Final aggregation
result_combined.count()
time_combined = time.time() - start

print(f"Time with combined approach: {time_combined:.2f}s")
improvement = ((time_skewed - time_combined) / time_skewed) * 100
print(f"⚡ Performance improvement: {improvement:.1f}%")

# =====================================================
# SUMMARY AND ANALYSIS
# =====================================================
print("\n" + "=" * 80)
print("PERFORMANCE SUMMARY")
print("=" * 80)

results = [
    ("Original (Skewed)", time_skewed, 0),
    ("Salting", time_salted, ((time_skewed - time_salted) / time_skewed) * 100),
    ("Repartitioning", time_repart, ((time_skewed - time_repart) / time_skewed) * 100),
    ("reduceByKey", time_reduce, ((time_skewed - time_reduce) / time_skewed) * 100),
    ("Combined (Best)", time_combined, ((time_skewed - time_combined) / time_skewed) * 100)
]

print(f"\n{'Method':<25} {'Time (s)':<12} {'Improvement':<15}")
print("-" * 52)
for method, time_val, improvement in results:
    imp_str = f"+{improvement:.1f}%" if improvement > 0 else "baseline"
    print(f"{method:<25} {time_val:<12.2f} {imp_str:<15}")

print("\n" + "=" * 80)
print("KEY INSIGHTS AND ANALYSIS")
print("=" * 80)
print("""
1. DATA SKEW PROBLEM:
   - 90% of data in one partition creates a bottleneck
   - Other partitions finish quickly but wait for the slow one
   - Overall job time = time of slowest partition

2. SALTING TECHNIQUE:
   - Adds random suffix to hot keys (e.g., "key" → "key_salt_0", "key_salt_1")
   - Distributes skewed key across multiple partitions
   - Requires extra reduce step to combine salted results
   - Best for: Extremely skewed keys

3. REPARTITIONING:
   - Increases partition count for better distribution
   - Simple but causes shuffle overhead
   - May not solve extreme skew (90% still goes to one partition)
   - Best for: Moderate skew

4. REDUCEBYKEY vs GROUPBYKEY:
   - reduceByKey combines values on mapper before shuffle
   - groupByKey shuffles all data then combines
   - reduceByKey = less network transfer = faster
   - Best practice: Always prefer reduceByKey when possible

5. COMBINED APPROACH (BEST):
   - Salting breaks up hot keys
   - reduceByKey minimizes shuffle
   - Two-stage aggregation: salted → final
   - Best for: Production systems with skewed data

DETECTION STRATEGIES:
- Monitor partition sizes with mapPartitionsWithIndex
- Check key distribution with countByKey()
- Look for stragglers in Spark UI (one slow task)
- Profile execution time per partition

WHEN TO USE EACH:
- Mild skew (70/30): Just use reduceByKey
- Moderate skew (80/20): Repartitioning + reduceByKey
- Severe skew (90/10+): Salting + reduceByKey
- Extreme skew (95/5+): Salting + broadcast join + reduceByKey
""")

print("\n" + "=" * 80)
print("RECOMMENDATIONS FOR YOUR DATA")
print("=" * 80)
print(f"""
Based on your skew ratio (90/10):
✓ Use salting for hot keys
✓ Apply reduceByKey instead of groupByKey
✓ Consider broadcast join if joining with small tables
✓ Monitor with Spark UI for partition-level metrics
✓ Expected improvement: {results[-1][2]:.1f}%
""")

sc.stop()