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

#Total Distinct Occupation Count of the User
occupation_count = users_df.select("occupation").distinct().count()

#Total user
total_count = users_df.count()

#Average  Age of User
average_age = users_df.select(avg("age")).first()[0]

#Minimum age of user
min_age = users_df.select(min("age")).first()[0]

#Maximum age of user
max_age = users_df.select(max("age")).first()[0]

#Gender Count
gender_count = users_df.groupBy("gender").count().orderBy('count').collect()

#Count of Every Occupation
occupation_count = users_df.groupBy("occupation").count().orderBy('count').collect()

#Displaying the results

print(f"Total count of user_id: {total_count}")

print(f"Average age of users: {average_age}")

print(f"Minimum age of users: {min_age}")

print(f"Maximum age of users: {max_age}")

for row in gender_count:
    print(f"{row['gender']}: {row['count']}")
print("Count of each occupation:")
for row in occupation_count:
    print(f"{row['occupation']}: {row['count']}")

#CALCULATING THE RATING COUNT PROVIDED BY EACH USER
#EACH USER HIGHEST, LOWEST AND AVERAGE RATING OF EACH USER

user_rating_count = data_df.groupBy("user id").agg(count("rating").alias("rating_count"))

user_highest_rating = data_df.groupBy("user id").agg(max("rating").alias("highest_rating"))

user_lowest_rating = data_df.groupBy("user id").agg(min("rating").alias("lowest_rating"))

user_avg_rating = data_df.groupBy("user id").agg(avg("rating").alias("average_rating"))

#Displaying the user highest, lowesta and average rating
user_ratings_analysis = user_rating_count.join(user_highest_rating, "user id") \
    .join(user_lowest_rating, "user id") \
    .join(user_avg_rating, "user id")

user_ratings_analysis.show()

#FINDING THE USER ID THAT HAS PROVIDED MAXIMUM RATING COUNT AND MINIMUM RATING COUNT
max_rated_user = user_ratings_analysis.orderBy(col("rating_count").desc()).first()
print("User with maximum rated movies:", max_rated_user)

min_rated_user = user_ratings_analysis.orderBy(col("rating_count")).first()
print("User with minimum rated movies:", min_rated_user)

#HIGHEST RATED MOVIE BY MAXIMUM USERS, LOWEST RATED MOVIE BY MAXIMUM USER
highest_rated_movie = data_df.filter(col("rating") == 5).groupBy("movie id").agg(count("user id").alias("count")).orderBy(col("count").desc()).first()

lowest_rated_movie = data_df.filter(col("rating") == 1).groupBy("movie id").agg(count("user id").alias("count")).orderBy(col("count").desc()).first()

highest_rated_movie_title = items_df.filter(col("movie id") == highest_rated_movie["movie id"]).select("movie title").first()

lowest_rated_movie_title = items_df.filter(col("movie id") == lowest_rated_movie["movie id"]).select("movie title").first()

print("Movie rated highest by the maximum number of users:", highest_rated_movie_title["movie title"])

print("Movie rated lowest by the maximum number of users:", lowest_rated_movie_title["movie title"])


