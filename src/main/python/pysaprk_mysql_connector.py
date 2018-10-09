"""
In Pycharm, we need to copy relevant jdbc jar file to SPARK_HOME/jars
We can either use spark.read.format(‘jdbc’) with options or spark.read.jdbc with jdbc url,
table name and other properties as dict to read data from remote relational databases.
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('getting started'). \
    getOrCreate()

# car_orders = spark.read. \
#     format('jdbc'). \
#     option('url', 'jdbc:mysql://127.0.0.1:3306'). \
#     option('dbtable', 'classicmodels.orders'). \
#     option('user', 'root'). \
#     option('password', 'suntv'). \
#     load()

orders = spark.read. \
    jdbc('jdbc:mysql://ms.itversity.com',
         'retail_db.orders',
         numPartitions=4,
         properties={'user':'retail_user','password':'itversity'})

orders.printSchema()

orderdetails = spark.read. \
    jdbc('jdbc:mysql://127.0.0.1:3306',
         'classicmodels.orderdetails',
         numPartitions=4,
         properties={'user':'root','password':'suntv'})

orderdetails.printSchema()

order_item_revenue_per_order_id  = spark.read. \
    jdbc('jdbc:mysql://ms.itversity.com',
         '(select order_item_order_id, round(sum(order_item_subtotal), 2) order_revenue '
         'from retail_db.order_items group by order_item_order_id) q',
         properties ={'user':'retail_user', 'password':'itversity'})
order_item_revenue_per_order_id.show()


query = "(select order_status, count(1) from retail_db.orders group by order_status) t"
queryData = spark.read. \
    jdbc("jdbc:mysql://ms.itversity.com", query,
         properties={"user": "retail_user",
                     "password": "itversity"})

queryData.show()
