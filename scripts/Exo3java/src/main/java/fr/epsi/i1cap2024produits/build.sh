#!/bin/bash

# Définir les variables
PROJECT_NAME="i1cap2024produits"

JAR_NAME="Exo3java-1.0-SNAPSHOT.jar"
INPUT_DIR="input"
OUTPUT_DIR="output"
HADOOP_HOME="/path/to/hadoop"  # Remplacez ceci par le chemin vers votre installation Hadoop

# Step 3: Put the JAR and CSV on HDFS
echo "Putting the CSV file on HDFS..."
hdfs dfs -mkdir -p $HDFS_INPUT_DIR
hdfs dfs -put -f $LOCAL_CSV_FILE $HDFS_INPUT_DIR

# Step 4: Run the Hadoop job
echo "Running the Hadoop job..."
hadoop jar $JAR_FILE fr.epsi.i1cap2024produits.CSVProcessingJob $HDFS_CSV_FILE $HDFS_OUTPUT_DIR


echo "Job completed. Output can be found in: $HDFS_OUTPUT_DIR"