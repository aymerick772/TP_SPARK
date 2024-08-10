from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, when, regexp_replace
from pyspark.sql.window import Window

# Initialiser la session Spark
spark = SparkSession.builder.appName("ArbresRemarquables").getOrCreate()

# Charger les données CSV dans un DataFrame Spark
df = spark.read.option("delimiter", ";").csv("hdfs://hadoop-master:9000/input/arbresremarquablesparis.csv", header=True)

# Vérifier les noms de colonnes
print("Noms des colonnes :", df.columns)
# Renommer les colonnes pour correspondre aux noms attendus
df = df.withColumnRenamed("Geo point", "Geo_point") \
       .withColumnRenamed("hauteur en m", "hauteur_m") \
       .withColumnRenamed("circonference en cm", "circonference_cm") \
       .withColumnRenamed("arrondissement3", "arrondissement_none") \
       .withColumnRenamed("adresse6", "adresse") \
       .withColumnRenamed("genre", "genre") \
       .withColumnRenamed("espèce", "espece") \
       .withColumnRenamed("Arrondissement21", "arrondissement") \
       .withColumnRenamed("Adresse19", "adresse_none") \
       .withColumnRenamed("Domanialité", "domanialite") \
       .withColumnRenamed("Dénomination usuelle", "denomination_usuelle") \
       .withColumnRenamed("Dénomination botanique", "denomination_botanique")

# Nettoyer les données
df_filt = df.filter(col("arrondissement") != '1er')


# Vérifier à nouveau les noms des colonnes après renommage
print("Colonnes après renommage :", df.columns)
# a. Afficher les coordonnées GPS, la taille (hauteur) et l'adresse de l'arbre le plus grand
df_filtered_hauteur = df_filt.filter(col("hauteur_m") != '').withColumn("hauteur_m", col("hauteur_m").cast("float"))
arbre_plus_grand = df_filtered_hauteur.orderBy(desc("hauteur_m")).limit(1)
arbre_plus_grand_pd = arbre_plus_grand.toPandas()

# Vérifier les colonnes du DataFrame Pandas
print("Colonnes du DataFrame Pandas pour l'arbre le plus grand :", arbre_plus_grand_pd.columns)

print("Arbre le plus grand :")
print(arbre_plus_grand_pd[['Geo_point', 'hauteur_m', 'genre', "arrondissement", "adresse"]])

# b. Afficher les coordonnées GPS, la taille (hauteur) et la circonférence des arbres les plus grands pour chaque arrondissement
df_filtered_circonference = df_filt.filter(col("circonference_cm") != '').withColumn("circonference_cm", col("circonference_cm").cast("float"))



df_sorted = df_filtered_circonference.orderBy(col("circonference_cm").desc())

# Afficher les résultats
print('c la')
df_sorted.show(truncate=False)
#afficher les coordonnées GPS des arbres de plus grandes circonférences pour chaque arrondissement de la ville de Paris.
df_sorted[['Geo_point', 'hauteur_m', 'genre', "arrondissement", "circonference_cm"]].show(truncate=False)


df_filtered_species = df.filter(col("genre").isNotNull() & col("espece").isNotNull())

# Trier les données par genre et espèce
df_sorted_species = df_filtered_species.orderBy(col("genre").asc(), col("espece").asc())

# Afficher les résultats
print('par genre')
df_sorted_species.select("genre", "espece").distinct().show(truncate=False)

spark.stop()