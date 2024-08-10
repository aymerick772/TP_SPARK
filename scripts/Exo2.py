from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("MovieLensAnalysis") \
    .getOrCreate()

# Chemins des fichiers CSV
links_path = "hdfs://hadoop-master:9000/input/ml-latest-small/links.csv"
movies_path = "hdfs://hadoop-master:9000/input/ml-latest-small/movies.csv"
ratings_path = "hdfs://hadoop-master:9000/input/ml-latest-small/ratings.csv"
tags_path = "hdfs://hadoop-master:9000/input/ml-latest-small/tags.csv"

# Charger les données
links_df = spark.read.csv(links_path, header=True, inferSchema=True)
movies_df = spark.read.csv(movies_path, header=True, inferSchema=True, sep=',')
ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True, sep=',')
tags_df = spark.read.csv(tags_path, header=True, inferSchema=True, sep=',')

# Joindre les données pour obtenir les informations des films avec les tags et les notes
movies_ratings_df = movies_df.join(ratings_df, on="movieId")
movies_ratings_tags_df = movies_ratings_df.join(tags_df, on=["movieId", "userId"], how="left")

# Calculer le nombre de tags pour chaque film
movies_tags_count_df = movies_ratings_tags_df.groupBy("movieId", "title", "genres").agg(count("tag").alias("tags_count"))

# Calculer la moyenne des notes pour chaque film
movies_ratings_avg_df = movies_ratings_df.groupBy("movieId", "title", "genres").agg(avg("rating").alias("avg_rating"))

# Joindre les deux DataFrames pour obtenir la moyenne des notes et le nombre de tags pour chaque film
movies_combined_df = movies_tags_count_df.join(movies_ratings_avg_df, on=["movieId", "title", "genres"])

# Séparer les genres multiples en lignes distinctes
from pyspark.sql.functions import explode, split

movies_genres_df = movies_combined_df.withColumn("genre", explode(split(col("genres"), "\\|")))

# Calculer la moyenne des notes pour chaque genre
genre_avg_rating_df = movies_genres_df.groupBy("genre").agg(avg("avg_rating").alias("avg_genre_rating"))

# Compter le nombre de tags pour chaque genre
genre_tags_count_df = movies_genres_df.groupBy("genre").agg(count("tags_count").alias("total_tags_count"))

# Joindre les DataFrames pour obtenir la moyenne des notes et le nombre total de tags pour chaque genre
genre_combined_df = genre_avg_rating_df.join(genre_tags_count_df, on="genre")

# Trier par la moyenne des notes et le nombre total de tags
result_df = genre_combined_df.orderBy(desc("avg_genre_rating"), desc("total_tags_count"))

# Afficher les résultats
result_df.show(truncate=False)
