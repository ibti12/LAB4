from pyspark import SparkContext

sc = SparkContext("local[*]", "MapPracticeLab")

# Exemple de données clients
customers_data = [
    {"id": 1, "name": "Alice", "country": "USA", "city": "New York", "segment": "Premium", "credit_limit": 80000},
    {"id": 2, "name": "Bob", "country": "Canada", "city": "Toronto", "segment": "Standard", "credit_limit": 30000},
    {"id": 3, "name": "Charlie", "country": "USA", "city": "Los Angeles", "segment": "Standard", "credit_limit": 20000},
]

# Création du RDD
customers_rdd = sc.parallelize(customers_data)

# PROBLEM 1: USA Customers
usa_customers = customers_rdd.filter(lambda c: c['country'] == "USA").collect()
print("USA Customers:", usa_customers)

# PROBLEM 2: City-Country tuples
city_country = customers_rdd.map(lambda c: (c['city'], c['country'])).collect()
print("City-Country tuples:", city_country)

# PROBLEM 3: Formatted IDs
formatted_ids = customers_rdd.map(lambda c: (c['id'], f"CUST-{c['id']:05d}")).collect()
print("Formatted IDs:", formatted_ids)

# PROBLEM 4: High-value customers
high_value = customers_rdd.filter(lambda c: c['credit_limit'] > 50000).collect()
print("High-value customers:", high_value)

# PROBLEM 5: Customer Summary by Tier
def get_tier(credit):
    if credit > 75000:
        return "High"
    elif credit >= 25000:
        return "Medium"
    else:
        return "Low"

customer_summary = customers_rdd.map(
    lambda c: (c['id'], c['name'], c['segment'], get_tier(c['credit_limit']))
).collect()
print("Customer Summary:", customer_summary)

# BONUS: Average credit by segment
avg_by_segment = (
    customers_rdd
    .map(lambda c: (c['segment'], (c['credit_limit'], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1])
    .collect()
)
print("Average credit by segment:", avg_by_segment)

sc.stop()
