from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf().setAppName("Day2-Filter").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("="*70)
print("FILTER OPERATIONS")
print("="*70)

# Load CSV
orders = sc.textFile(r"file:///C:/Users/pc/spark-ecommerce-lab/spark-data/ecommerce/orders.csv")


header = orders.first()
orders_data = orders.filter(lambda line: line != header)

def parse_order(line):
    fields = line.split(',')
    return {
        'order_id': int(fields[0]),
        'order_date': fields[1],
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5]),
        'payment_method': fields[6]
    }

orders_rdd = orders_data.map(parse_order)
print(f"Total orders: {orders_rdd.count()}")

# FILTER EXAMPLES
high_value = orders_rdd.filter(lambda o: o['amount'] > 5000)
print(f"High-value orders (>5000): {high_value.count()}")

shipped_high = orders_rdd.filter(lambda o: o['status']=='Shipped' and o['amount']>2000)
print(f"Shipped & >$2000: {shipped_high.count()}")

def problem_order(o):
    return o['status'] in ['On Hold','Cancelled'] and o['amount']>1000
problem_orders = orders_rdd.filter(problem_order)
print(f"Problem orders: {problem_orders.count()}")

def in_q4_2024(o):
    d = datetime.strptime(o['order_date'], '%Y-%m-%d')
    return d.year==2024 and d.month>=10
q4_orders = orders_rdd.filter(in_q4_2024)
print(f"Q4 2024 orders: {q4_orders.count()}")

sample_orders = orders_rdd.sample(False, 0.1, 42)
print(f"Sampled ~10% orders: {sample_orders.count()}")

# Practice solutions
range_orders = orders_rdd.filter(lambda o: 1000 <= o['amount'] <= 5000)
processing = orders_rdd.filter(lambda o: o['status']=='Processing')
early_customers = orders_rdd.filter(lambda o: 1 <= o['customer_id'] <= 100)
paypal_high = orders_rdd.filter(lambda o: o['payment_method']=='PayPal' and o['amount']>2000)

sc.stop()
