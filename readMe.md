# Analyse des Arbres Remarquables de Paris avec PySpark

Ce projet utilise **PySpark** pour analyser un ensemble de données sur les arbres remarquables de Paris. Les données proviennent d'un fichier CSV contenant diverses informations sur les arbres, telles que leurs coordonnées GPS, leur hauteur, leur circonférence, et plus encore.

## Prérequis

Avant de commencer, assurez-vous d'avoir :

- **Docker** installé et configuré pour exécuter un conteneur Hadoop/Spark.
- Un conteneur Docker en cours d'exécution avec Hadoop et Spark (nommé `hadoop-master`).
- **PySpark** installé dans le conteneur.

## Étapes pour Exécuter les Projets

```bash
#Exo 1
docker cp "C:\Users\monpc\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\arbresremarquablesparis.csv" hadoop-master:/root/
docker cp "C:\Users\monpc\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\scripts\pyspark_analysis.py" hadoop-master:/root/



#Exo 2
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip
rm ml-latest-small.zip

#Copier dans le container le code 
docker cp "C:\Users\aymer\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\scripts\Exo2.py" hadoop-master:/root/
#Mettre dans le cichier hdfs 
hadoop fs -put /root/ml-latest-small /input/
#voir dans le fichier hdfs 
hdfs dfs -ls /input/ml-latest-small/
#Lancer le script Exo2
spark-submit /root/Exo2.py




#Exo 3 
#Analyse de la démographie en France en 2023
docker cp "C:\Users\aymer\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\scripts\Exo3java\target\Exo3java-1.0-SNAPSHOT.jar" hadoop-master:/root/
docker cp "C:\Users\aymer\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\scripts\Exo3java\src\main\java\fr\epsi\i1cap2024produits\build.sh" hadoop-master:/root/
docker cp "C:\Users\aymer\Desktop\PRO\ETUDES\EPSI\cours\ALT_BIG_DATA\BigData\TP_rendu\scripts\Exo3java\Donnees\demographie_france_2023.csv" hadoop-master:/root/

hadoop fs -put /root/demographie_france_2023.csv /input/



