# lab2_map_operations.py
from pyspark import SparkContext

# Initialiser SparkContext
sc = SparkContext("local[*]", "MapOperationsLab")

print("="*70)
print("LAB 2 — MAP OPERATIONS")
print("="*70)

# Exemple de données
numbers = list(range(1, 11))  # 1 à 10
numbers_rdd = sc.parallelize(numbers, numSlices=3)

# 1️⃣ map(): transformer chaque élément
squared_rdd = numbers_rdd.map(lambda x: x**2)
print("[map] Squares:", squared_rdd.collect())

# 2️⃣ mapPartitions(): transformation par partition
def sum_partition(iterable):
    total = sum(iterable)
    yield total

partition_sums = numbers_rdd.mapPartitions(sum_partition)
print("[mapPartitions] Sum per partition:", partition_sums.collect())

# 3️⃣ mapPartitionsWithIndex(): inclure l'index de partition
def partition_info(index, iterator):
    yield (index, list(iterator))

info_rdd = numbers_rdd.mapPartitionsWithIndex(partition_info)
print("[mapPartitionsWithIndex] Partition contents:", info_rdd.collect())

# 4️⃣ flatMap(): un élément → plusieurs éléments
words = ["hello world", "apache spark", "pyspark map flatMap"]
words_rdd = sc.parallelize(words)

words_flat = words_rdd.flatMap(lambda line: line.split())
print("[flatMap] All words:", words_flat.collect())

# 5️⃣ mapValues(): utilisé sur les RDD de type (clé, valeur)
kv_rdd = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
kv_squared = kv_rdd.mapValues(lambda v: v**2)
print("[mapValues] Squared values:", kv_squared.collect())

# 6️⃣ collect(): récupérer les données sur le driver
collected_numbers = numbers_rdd.collect()
print("[collect] Numbers:", collected_numbers)

# 7️⃣ mapPartitions + resource example
# Exemple: on peut ouvrir un fichier ou connexion DB par partition
def example_partition_resource(iterable):
    # Simule une "connexion" par partition
    result = [x*10 for x in iterable]
    return result

resource_rdd = numbers_rdd.mapPartitions(example_partition_resource)
print("[mapPartitions + resource] Times 10:", resource_rdd.collect())

sc.stop()
