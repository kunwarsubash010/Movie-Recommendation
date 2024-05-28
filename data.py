#venv -> virtual env name of this one

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, count, col
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Movie Recommendation").getOrCreate()

users_df = spark.read.csv("/Users/shubhash/Desktop/Movie/Movie-Recommendation/ml-100k/u.user", sep='|', header=False, inferSchema=True)
items_df = spark.read.csv("/Users/shubhash/Desktop/Movie/Movie-Recommendation/ml-100k/u.item", sep='|', header=False, inferSchema=True)
data_df = spark.read.csv("/Users/shubhash/Desktop/Movie/Movie-Recommendation/ml-100k/u.data", sep='\t', header=False, inferSchema=True)


user_cols = ['user id','age','gender','occupation','zip code']
item_cols = ['movie id','movie title','release date','video release date','IMDb URL','unknown','Action','Adventure','Animation',
             'Childrens','Comedy','Crime','Documentary','Drama','Fantasy','Film-Noir','Horror','Musical','Mystery','Romance ',
             'Sci-Fi','Thriller','War' ,'Western']

data_cols = ['user id','movie id','rating','timestamp']


users_df = users_df.toDF(*user_cols)
items_df = items_df.toDF(*item_cols)
data_df = data_df.toDF(*data_cols)


users_df.show(5)
items_df.show(5)
data_df.show(5)


