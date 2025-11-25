from pyspark import SparkContext, SparkConf

# Spark configuration
conf = SparkConf().setAppName("Day2-Joins").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("JOIN OPERATIONS")
print("=" * 70)

# =====================================================
# LOAD AND PREPARE DATA
# =====================================================

def load_customers():
    customers = sc.textFile("file:///C:/Users/pc/spark-ecommerce-lab/spark-data/ecommerce/customers.csv")
    header = customers.first()
    data = customers.filter(lambda line: line != header)
    
    def parse(line):
        fields = line.split(',')
        return (int(fields[0]), {
            'name': fields[1],
            'city': fields[6],
            'country': fields[8],
            'segment': fields[10]
        })
    
    return data.map(parse)

def load_orders():
    orders = sc.textFile("file:///C:/Users/pc/spark-ecommerce-lab/spark-data/ecommerce/orders.csv")
    header = orders.first()
    data = orders.filter(lambda line: line != header)
    
    def parse(line):
        fields = line.split(',')
        return (int(fields[4]), {
            'order_id': int(fields[0]),
            'order_date': fields[1],
            'status': fields[3],
            'amount': float(fields[5])
        })
    
    return data.map(parse)

customers = load_customers()
orders = load_orders()

print(f"Customers: {customers.count()}")
print(f"Orders: {orders.count()}")

# =====================================================
# JOIN 1: Inner Join
# =====================================================
print("\n[JOIN 1] Inner Join - Orders with customer info")
joined = customers.join(orders)
for customer_id, (cust, order) in joined.take(5):
    print(f"Customer {customer_id}: {cust['name']:30s} | Order #{order['order_id']}: ${order['amount']:.2f}")

# =====================================================
# JOIN 2: Left Outer Join
# =====================================================
print("\n[JOIN 2] Left Outer Join - All customers with/without orders")
left_joined = customers.leftOuterJoin(orders)
customers_without_orders = left_joined.filter(lambda x: x[1][1] is None)
print(f"Customers WITHOUT orders: {customers_without_orders.count()}")
for customer_id, (cust, order) in customers_without_orders.take(5):
    print(f"Customer {customer_id}: {cust['name']} from {cust['city']}")

# =====================================================
# JOIN 3: Right Outer Join
# =====================================================
print("\n[JOIN 3] Right Outer Join - All orders, even without customer data")
right_joined = orders.rightOuterJoin(customers)
orphaned_orders = right_joined.filter(lambda x: x[1][0] is None)
print(f"Orphaned orders (no customer): {orphaned_orders.count()}")

# =====================================================
# JOIN 4: Full Outer Join
# =====================================================
print("\n[JOIN 4] Full Outer Join - Everything")
full_joined = customers.fullOuterJoin(orders)
both_present = full_joined.filter(lambda x: x[1][0] is not None and x[1][1] is not None)
only_customer = full_joined.filter(lambda x: x[1][0] is not None and x[1][1] is None)
only_order = full_joined.filter(lambda x: x[1][0] is None and x[1][1] is not None)
print(f"Records with both customer and order: {both_present.count()}")
print(f"Records with only customer: {only_customer.count()}")
print(f"Records with only order: {only_order.count()}")

# =====================================================
# JOIN 7: Broadcast Join (Small + Large)
# =====================================================
print("\n[JOIN 7] Broadcast join optimization")
status_descriptions = sc.parallelize([
    ("Shipped", "Order has been shipped to customer"),
    ("Processing", "Order is being processed"),
    ("On Hold", "Order is temporarily on hold"),
    ("Cancelled", "Order was cancelled")
])
status_map = sc.broadcast(dict(status_descriptions.collect()))
orders_with_desc = orders.mapValues(lambda o: {**o, 'status_desc': status_map.value.get(o['status'], 'Unknown')})
for customer_id, order in orders_with_desc.take(5):
    print(f"Order #{order['order_id']}: {order['status']:12s} - {order['status_desc']}")

sc.stop()
