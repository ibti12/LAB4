from pyspark import SparkContext, SparkConf
from datetime import datetime

# Spark configuration
conf = SparkConf().setAppName("Day2-Aggregations").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("ADVANCED AGGREGATIONS")
print("=" * 70)

# Load orders
orders = sc.textFile("file:///C:/Users/pc/spark-ecommerce-lab/spark-data/ecommerce/orders.csv")
header = orders.first()
orders_data = orders.filter(lambda line: line != header)

def parse_order(line):
    fields = line.split(',')
    return {
        'order_id': int(fields[0]),
        'order_date': datetime.strptime(fields[1], '%Y-%m-%d'),
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5])
    }

orders_rdd = orders_data.map(parse_order)

# =====================================================
# AGGREGATION 1: Basic Statistics
# =====================================================
amounts = orders_rdd.map(lambda o: o['amount'])
total = amounts.sum()
count = amounts.count()
average = amounts.mean()
min_val = amounts.min()
max_val = amounts.max()
stdev = amounts.stdev()

print(f"Total Orders: {count}")
print(f"Total Revenue: ${total:,.2f}")
print(f"Average Order: ${average:.2f}")
print(f"Min Order: ${min_val:.2f}")
print(f"Max Order: ${max_val:.2f}")
print(f"Std Deviation: ${stdev:.2f}")

# =====================================================
# AGGREGATION 3: Monthly revenue trends
# =====================================================
monthly_revenue = orders_rdd \
    .map(lambda o: (f"{o['order_date'].year}-{o['order_date'].month:02d}", o['amount'])) \
    .reduceByKey(lambda a, b: a + b) \
    .sortByKey()

print("\nMonthly Revenue:")
for month, revenue in monthly_revenue.collect():
    print(f"{month}: ${revenue:,.2f}")

# =====================================================
# AGGREGATION 5: RFM Analysis
# =====================================================
reference_date = datetime(2024, 12, 1)
rfm = orders_rdd \
    .map(lambda o: (o['customer_id'], (o['order_date'], o['amount']))) \
    .aggregateByKey(
        (reference_date, 0, 0),
        lambda acc, val: (
            max(acc[0], val[0]) if acc[0] != reference_date else val[0],
            acc[1] + 1,
            acc[2] + val[1]
        ),
        lambda acc1, acc2: (
            max(acc1[0], acc2[0]),
            acc1[1] + acc2[1],
            acc1[2] + acc2[2]
        )
    ) \
    .mapValues(lambda x: {
        'recency_days': (reference_date - x[0]).days,
        'frequency': x[1],
        'monetary': x[2]
    })

print("\nSample RFM scores:")
for customer_id, rfm_scores in rfm.take(10):
    print(f"Customer {customer_id}: R={rfm_scores['recency_days']}d, "
          f"F={rfm_scores['frequency']} orders, M=${rfm_scores['monetary']:,.2f}")

sc.stop()
