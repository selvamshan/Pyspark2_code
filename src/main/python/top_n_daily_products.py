from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import *
from pyspark.sql.window import  Window
import configparser as cp
import sys

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")

env = sys.argv[1]
print(env)

spark = SparkSession. \
    builder. \
    master(props.get(env, 'executionMode')). \
    appName('Daily product per revenue'). \
    getOrCreate()

input_base_dir = props.get(env, 'input.base.dir')
output_base_dir = props.get(env, 'output.base.dir')

orders_csv = spark. \
    read.csv(input_base_dir + '/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

orders = orders_csv. \
    withColumn('order_id', orders_csv.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', orders_csv.order_customer_id.cast(IntegerType()))

# orders.printSchema()
# orders.show()

order_items_csv = spark. \
    read.csv(input_base_dir + '/order_items'). \
    toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
         'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

order_items = order_items_csv. \
    withColumn('order_item_id', order_items_csv.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', order_items_csv.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', order_items_csv.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', order_items_csv.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', order_items_csv.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', order_items_csv.order_item_product_price.cast(FloatType()))

# order_items.printSchema()

spark.conf.set('spark.sql.shuffle.partitions', 2)

daily_product_revenue = orders. \
    where("order_status in ('COMPLETE','CLOSED')"). \
    join(order_items, orders.order_id == order_items.order_item_order_id). \
    groupBy('order_date', 'order_item_product_id'). \
    agg(round(sum('order_item_subtotal'), 2).alias('order_revenue'))

#daily_product_revenue.show()

spec = Window. \
    partitionBy(daily_product_revenue.order_date). \
    orderBy(daily_product_revenue.order_revenue.desc())

spark.conf.set('spark.sql.shuffle.partitions', '2')

daily_product_revenue_ranked = daily_product_revenue. \
    withColumn('rnk', rank().over(spec))

top_n = int(sys.argv[2])

topN_daily_products = daily_product_revenue_ranked. \
    where(daily_product_revenue_ranked.rnk <= top_n). \
    orderBy('order_date', daily_product_revenue.order_revenue.desc()). \
    drop('rnk')



topN_daily_products.write.csv(output_base_dir + '/topN_daily_products')

#video 1:31

"""
spark-submit --master yarn  \
--deploy-mode client \
--num-executors 1 \
topN_daily_products.py prod 5

"""