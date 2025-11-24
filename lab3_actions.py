import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Day1-Actions")
    .master("local[*]")
    .getOrCreate()
)

customers = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("spark-data/ecommerce/customers.csv")
)

# ------------------------------------------------------
# Action 1 — count()
# ------------------------------------------------------
print("\nTotal customers:", customers.count())
enterprise = customers.filter(customers.customerSegment == "Enterprise")

print("Enterprise customers:", enterprise.count())

# ------------------------------------------------------
# Action 2 — show()
# ------------------------------------------------------
print("\nDefault show()")
customers.show()

print("\nshow(5, truncate=False)")
customers.show(5, truncate=False)

print("\nshow(3, vertical=True)")
customers.show(3, vertical=True)

# ------------------------------------------------------
# Action 3 — collect()
# ------------------------------------------------------
print("\n=== collect() small sample ===")
small_sample = customers.limit(3).collect()
print("Type of collect:", type(small_sample))
print("Type of element:", type(small_sample[0]))

for r in small_sample:
    print(r.customerName, "-", r.country)

print("\nWARNING: collect() brings ALL data to driver — dangerous for big datasets!")

# ------------------------------------------------------
# Action 4 — take(n)
# ------------------------------------------------------
print("\n=== take(5) ===")
rows = customers.take(5)
for r in rows:
    print(f"{r.customerName} | {r.country}")

# ------------------------------------------------------
# Action 5 — first() and head()
# ------------------------------------------------------
print("\nfirst():", customers.first().customerName)
print("head():", customers.head().customerName)
print("Note: on a DataFrame, first() == head()")

# ------------------------------------------------------
# Action 6 — write()
# ------------------------------------------------------
usa = customers.filter(customers.country == "USA")

usa.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "spark-data/ecommerce/usa_customers"
)

customers.write.mode("overwrite").parquet(
    "spark-data/ecommerce/customers_parquet"
)

# ------------------------------------------------------
# Action 7 — foreachPartition()
# ------------------------------------------------------
def inspect_partition(iterator):
    size = sum(1 for _ in iterator)
    print(f">>> Partition size = {size}")
print("\n=== foreachPartition inspection ===")

def inspect_partition(rows):
    print("Partition preview:")
    for r in list(rows)[:3]:
        print(r)

for r in customers.take(5):
    print(r)


# ------------------------------------------------------
# PERFORMANCE TEST
# ------------------------------------------------------
print("\n=== PERFORMANCE MEASUREMENTS ===")

t0 = time.time()
customers.count()
print("count() time:", time.time() - t0, "sec")

t0 = time.time()
customers.show(5)
print("show() time:", time.time() - t0, "sec")

t0 = time.time()
customers.limit(100).collect()
print("collect() time:", time.time() - t0, "sec")

print("""
Dangerous patterns:
• df.collect() on large data
• df.toPandas() on large data
• for row in df.collect(): ...
Safer alternatives:
• limit().collect()
• take(n)
• show()
• foreachPartition()
• write.parquet()
""")

spark.stop()
