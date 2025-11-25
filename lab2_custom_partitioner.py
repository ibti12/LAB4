from pyspark import SparkContext, SparkConf
import hashlib

conf = SparkConf().setAppName("Lab2-DataSkew").setMaster("local[*]")

sc = SparkContext(conf=conf)

print("=" * 80)
print("LAB 2: CUSTOM PARTITIONER STRATEGIES")
print("=" * 80)

# =====================================================
# PART 1: Understanding Default Hash Partitioner
# =====================================================
print("\n[PART 1] Default Hash Partitioner\n")

# Create sample e-commerce data
# Format: (customer_id, order_value)
orders = [
    (101, 250.50), (102, 175.00), (101, 320.00), (103, 89.99),
    (104, 450.00), (101, 125.75), (105, 299.99), (102, 540.00),
    (106, 95.50), (103, 200.00), (107, 380.00), (104, 150.00),
    (108, 275.00), (105, 425.50), (109, 190.00), (110, 310.00),
    (101, 88.00), (102, 505.00), (111, 225.00), (112, 415.00)
]

orders_rdd = sc.parallelize(orders, numSlices=4)

print(f"Total orders: {len(orders)}")
print(f"Unique customers: {len(set(c for c, _ in orders))}")
print(f"Number of partitions: {orders_rdd.getNumPartitions()}")

# Analyze default distribution
def show_partition_contents(index, iterator):
    items = list(iterator)
    customer_ids = [c for c, _ in items]
    yield (index, len(items), customer_ids)

print("\nDefault Hash Partitioner Distribution:")
partition_info = orders_rdd.mapPartitionsWithIndex(show_partition_contents).collect()
for part_id, count, customers in partition_info:
    print(f"  Partition {part_id}: {count:2d} orders, Customers: {sorted(set(customers))}")

# =====================================================
# PART 2: Custom Range Partitioner
# =====================================================
print("\n[PART 2] Custom Range Partitioner\n")

class RangePartitioner:
    """
    Partitions customers into ranges:
    - Partition 0: Customer IDs 101-104 (Small customers)
    - Partition 1: Customer IDs 105-108 (Medium customers)
    - Partition 2: Customer IDs 109-112 (Large customers)
    """
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def __call__(self, key):
        # Customer ID based ranges
        if key <= 104:
            return 0
        elif key <= 108:
            return 1
        else:
            return 2

print("Applying Range Partitioner...")
range_partitioned = orders_rdd.partitionBy(3, RangePartitioner(3))

print("\nRange Partitioner Distribution:")
partition_info = range_partitioned.mapPartitionsWithIndex(show_partition_contents).collect()
for part_id, count, customers in partition_info:
    print(f"  Partition {part_id}: {count:2d} orders, Customers: {sorted(set(customers))}")

print("\n✓ Benefit: Customers grouped by ranges - useful for range queries")

# =====================================================
# PART 3: Custom Geography-Based Partitioner
# =====================================================
print("\n[PART 3] Geography-Based Partitioner\n")

# Enhanced data with country codes
geo_orders = [
    ("USA_101", 250.50), ("UK_201", 175.00), ("USA_102", 320.00),
    ("CA_301", 89.99), ("USA_103", 450.00), ("UK_202", 125.75),
    ("FR_401", 299.99), ("USA_104", 540.00), ("CA_302", 95.50),
    ("UK_203", 200.00), ("FR_402", 380.00), ("USA_105", 150.00),
    ("DE_501", 275.00), ("UK_204", 425.50), ("CA_303", 190.00),
    ("FR_403", 310.00), ("USA_106", 88.00), ("DE_502", 505.00)
]

geo_rdd = sc.parallelize(geo_orders, numSlices=5)

class GeoPartitioner:
    """
    Partitions by country:
    - Partition 0: USA
    - Partition 1: UK
    - Partition 2: Canada (CA)
    - Partition 3: France (FR)
    - Partition 4: Germany (DE)
    """
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
        self.country_map = {
            'USA': 0, 'UK': 1, 'CA': 2, 'FR': 3, 'DE': 4
        }
    
    def __call__(self, key):
        country = key.split('_')[0]
        return self.country_map.get(country, 0)

print("Applying Geography-Based Partitioner...")
geo_partitioned = geo_rdd.partitionBy(5, GeoPartitioner(5))

def show_geo_partition(index, iterator):
    items = list(iterator)
    countries = [k.split('_')[0] for k, _ in items]
    total_value = sum(v for _, v in items)
    yield (index, len(items), set(countries), total_value)

print("\nGeography-Based Distribution:")
geo_info = geo_partitioned.mapPartitionsWithIndex(show_geo_partition).collect()
country_names = {0: "USA", 1: "UK", 2: "Canada", 3: "France", 4: "Germany"}
for part_id, count, countries, total in geo_info:
    country_name = country_names.get(part_id, "Unknown")
    if count > 0:
        print(f"  Partition {part_id} ({country_name:7s}): {count:2d} orders, ${total:7.2f}")

print("\n✓ Benefit: Country-specific processing, regulatory compliance, geo-analytics")

# =====================================================
# PART 4: Custom Modulo Partitioner (Even Distribution)
# =====================================================
print("\n[PART 4] Custom Modulo Partitioner\n")

class ModuloPartitioner:
    """
    Distributes keys evenly using modulo operation
    Good for load balancing when key distribution is unknown
    """
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def __call__(self, key):
        # Extract numeric part from key
        if isinstance(key, str):
            key_num = int(key.split('_')[-1])
        else:
            key_num = key
        return key_num % self.num_partitions

print("Applying Modulo Partitioner...")
modulo_partitioned = orders_rdd.partitionBy(4, ModuloPartitioner(4))

print("\nModulo Partitioner Distribution:")
partition_info = modulo_partitioned.mapPartitionsWithIndex(show_partition_contents).collect()
for part_id, count, customers in partition_info:
    print(f"  Partition {part_id}: {count:2d} orders, Customers: {sorted(set(customers))}")

print("\n✓ Benefit: Guaranteed even distribution across partitions")

# =====================================================
# PART 5: Hash-Based Partitioner with Custom Hash Function
# =====================================================
print("\n[PART 5] Custom Hash Function Partitioner\n")

class CustomHashPartitioner:
    """
    Uses MD5 hash for consistent distribution
    Useful for string keys or when default hash causes skew
    """
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def __call__(self, key):
        # Use MD5 hash for better distribution
        hash_obj = hashlib.md5(str(key).encode())
        hash_val = int(hash_obj.hexdigest(), 16)
        return hash_val % self.num_partitions

print("Applying Custom Hash Partitioner...")
hash_partitioned = orders_rdd.partitionBy(4, CustomHashPartitioner(4))

print("\nCustom Hash Partitioner Distribution:")
partition_info = hash_partitioned.mapPartitionsWithIndex(show_partition_contents).collect()
for part_id, count, customers in partition_info:
    print(f"  Partition {part_id}: {count:2d} orders, Customers: {sorted(set(customers))}")

print("\n✓ Benefit: Better distribution for string keys, consistent hashing")

# =====================================================
# PART 6: Performance Comparison
# =====================================================
print("\n[PART 6] Performance Comparison\n")

import time

def measure_aggregation(rdd, name):
    start = time.time()
    result = rdd.reduceByKey(lambda a, b: a + b).collect()
    duration = time.time() - start
    return duration, result

print("Comparing aggregation performance with different partitioners...")

# Test each partitioner
results = []
for name, rdd in [
    ("Default Hash", orders_rdd),
    ("Range Based", range_partitioned),
    ("Modulo Based", modulo_partitioned),
    ("Custom Hash", hash_partitioned)
]:
    duration, _ = measure_aggregation(rdd, name)
    results.append((name, duration))
    print(f"  {name:20s}: {duration:.4f}s")

fastest = min(results, key=lambda x: x[1])
print(f"\n⚡ Fastest: {fastest[0]} ({fastest[1]:.4f}s)")

# =====================================================
# PART 7: Real-World Use Cases
# =====================================================
print("\n[PART 7] Real-World Use Cases\n")

print("""
USE CASE 1: Time-Series Data Partitioner
├─ Partition by date ranges (e.g., monthly)
├─ Enables efficient time-range queries
└─ Example: Log analysis, IoT sensor data

USE CASE 2: Alphabetical Partitioner
├─ Partition by first letter of name
├─ Useful for dictionary/phonebook applications
└─ Example: User directory, product catalog

USE CASE 3: Priority-Based Partitioner
├─ Separate high/medium/low priority items
├─ Process critical data in dedicated partitions
└─ Example: Order processing, support tickets

USE CASE 4: Hierarchical Partitioner
├─ Partition by organizational structure
├─ Department → Team → Individual
└─ Example: Enterprise data processing

USE CASE 5: Load-Balanced Partitioner
├─ Monitor partition sizes and rebalance
├─ Adaptive partitioning based on data volume
└─ Example: Real-time streaming applications
""")

# Example: Priority-Based Partitioner
print("\nExample Implementation: Priority-Based Partitioner\n")

priority_orders = [
    ("HIGH_001", 5000), ("LOW_002", 100), ("MED_003", 500),
    ("HIGH_004", 7500), ("LOW_005", 75), ("MED_006", 450),
    ("HIGH_007", 9000), ("LOW_008", 120), ("HIGH_009", 6000)
]

class PriorityPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def __call__(self, key):
        priority = key.split('_')[0]
        priority_map = {'HIGH': 0, 'MED': 1, 'LOW': 2}
        return priority_map.get(priority, 2)

priority_rdd = sc.parallelize(priority_orders, numSlices=3)
priority_partitioned = priority_rdd.partitionBy(3, PriorityPartitioner(3))

def show_priority_partition(index, iterator):
    items = list(iterator)
    priorities = [k.split('_')[0] for k, _ in items]
    total_value = sum(v for _, v in items)
    yield (index, len(items), set(priorities), total_value)

print("Priority-Based Distribution:")
priority_info = priority_partitioned.mapPartitionsWithIndex(show_priority_partition).collect()
priority_names = {0: "HIGH", 1: "MEDIUM", 2: "LOW"}
for part_id, count, priorities, total in priority_info:
    priority_name = priority_names.get(part_id, "Unknown")
    if count > 0:
        print(f"  Partition {part_id} ({priority_name:6s}): {count} orders, ${total:8.0f} total value")

# =====================================================
# SUMMARY
# =====================================================
print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)
print("""
1. DEFAULT HASH PARTITIONER:
   ✓ Good general-purpose distribution
   ✗ May cause skew with certain key patterns
   
2. RANGE PARTITIONER:
   ✓ Excellent for range queries (e.g., date ranges)
   ✓ Keeps related data together
   ✗ Can cause skew if ranges have uneven data

3. MODULO PARTITIONER:
   ✓ Guaranteed even distribution
   ✓ Simple and predictable
   ✗ Loses data locality

4. CUSTOM HASH PARTITIONER:
   ✓ Better distribution for string keys
   ✓ Consistent hashing properties
   ✗ Slightly more overhead

5. DOMAIN-SPECIFIC PARTITIONERS:
   ✓ Optimized for specific use cases
   ✓ Can leverage data semantics
   ✗ Requires domain knowledge

WHEN TO USE CUSTOM PARTITIONERS:
• Data has natural groupings (geography, time, category)
• Default partitioner causes skew
• Need co-location of related data
• Regulatory/compliance requirements (data locality)
• Performance optimization for specific queries

IMPLEMENTATION BEST PRACTICES:
• Keep partitioner logic simple and fast
• Ensure deterministic behavior (same key → same partition)
• Balance even distribution with data locality
• Test with production-like data volumes
• Monitor partition sizes in production
""")

sc.stop()