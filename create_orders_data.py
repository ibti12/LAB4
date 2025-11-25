import csv
import random
from datetime import datetime, timedelta
import os

os.makedirs("spark-data/ecommerce", exist_ok=True)

orders_data = []
statuses = ['Shipped', 'Processing', 'On Hold', 'Cancelled']
payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']
start_date = datetime(2024, 1, 1)

for i in range(1, 5001):  # 5000 orders
    order_date = start_date + timedelta(days=random.randint(0, 330))
    customer_id = random.randint(1, 1000)
    amount = round(random.uniform(100, 10000), 2)
    status = random.choice(statuses)
    payment = random.choice(payment_methods)
    orders_data.append([
        i,  # orderNumber
        order_date.strftime('%Y-%m-%d'),  # orderDate
        order_date.strftime('%Y-%m-%d'),  # requiredDate
        status,  # status
        customer_id,  # customerNumber
        amount,  # amount
        payment  # paymentMethod
    ])

# Write CSV
with open('spark-data/ecommerce/orders.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['orderNumber','orderDate','requiredDate','status','customerNumber','amount','paymentMethod'])
    writer.writerows(orders_data)

print(f"Orders data created successfully! Total records: {len(orders_data)}")
