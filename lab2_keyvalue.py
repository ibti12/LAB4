from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf().setAppName("Day2-KeyValue").setMaster("local[*]")
sc = SparkContext(conf=conf)

orders = sc.textFile(r"file:///C:/Users/pc/spark-ecommerce-lab/spark-data/ecommerce/orders.csv")

header = orders.first()
orders_data = orders.filter(lambda line: line != header)

def parse_order(line):
    fields = line.split(',')
    return {
        'order_id': int(fields[0]),
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5])
    }

orders_rdd = orders_data.map(parse_order)

# Pair RDD
customer_amounts = orders_rdd.map(lambda o: (o['customer_id'], o['amount']))

# mapValues
with_tax = customer_amounts.mapValues(lambda amt: amt*1.1)

# reduceByKey
total_per_customer = customer_amounts.reduceByKey(lambda a,b: a+b)
top_customers = total_per_customer.sortBy(lambda x: x[1], ascending=False)
print("Top 5 customers by total spending:", top_customers.take(5))

# groupByKey (warning: shuffle heavy)
grouped = customer_amounts.groupByKey()
print("Sample grouped orders:", [(k,list(v)) for k,v in grouped.take(2)])

# aggregateByKey - sum, count, min, max
stats_per_customer = customer_amounts.aggregateByKey(
    (0,0,float('inf'),float('-inf')),
    lambda acc,v: (acc[0]+v, acc[1]+1, min(acc[2],v), max(acc[3],v)),
    lambda acc1,acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1], min(acc1[2],acc2[2]), max(acc1[3],acc2[3]))
)
print("Stats for first 5 customers:", stats_per_customer.take(5))

# combineByKey - average per customer
def create_combiner(v): return (v,1)
def merge_value(acc,v): return (acc[0]+v, acc[1]+1)
def merge_combiners(acc1,acc2): return (acc1[0]+acc2[0], acc1[1]+acc2[1])
average_per_customer = customer_amounts.combineByKey(create_combiner, merge_value, merge_combiners)\
                                       .mapValues(lambda x: x[0]/x[1])
print("Top 5 avg per customer:", average_per_customer.take(5))

# Revenue by status
status_amounts = orders_rdd.map(lambda o: (o['status'], o['amount']))
revenue_by_status = status_amounts.reduceByKey(lambda a,b:a+b)
print("Revenue by status:", revenue_by_status.collect())

sc.stop()
