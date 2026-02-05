from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import pymongo
import os
import re

ATLAS_URI = "mongodb+srv://mspar02:mspar02@testcluster-mongodb.xvzubbe.mongodb.net/?appName=TestCluster-MongoDB"

# 1. INITIALISATION
spark = SparkSession.builder \
    .appName("Twitter_WorldCup_ETL") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def processus_etl_tweets(chemin_json):
    # IMPORT : Lecture du JSON brut
    df = spark.read.json(chemin_json)
    
    # EXTRACTION ET NETTOYAGE
    # On va chercher les champs dans la structure imbriquée
    df_clean = df.select(
        F.col("id_str").alias("tweet_id"),
        F.col("created_at"),
        F.col("user.screen_name").alias("user_handle"),
        F.col("user.followers_count").alias("followers"),
        F.col("user.location").alias("user_location"), # Utile pour mapper les régions
        F.col("text"),
        F.col("lang"),
        # La source pour ton graphique sur les appareils (iPhone, Android, etc.)
        F.col("source"), 
        # Extraction des hashtags
        F.col("entities.hashtags.text").alias("hashtags"),
        # Métadonnées d'engagement
        F.col("retweet_count"),
        F.col("favorite_count"),
        # Identification des retweets pour filtrer les contenus originaux
        F.col("retweeted_status.id_str").alias("is_retweet_id"),
        # Timestamp en millisecondes pour une conversion de date ultra-précise
        F.col("timestamp_ms")
    )

    # TRANSFORMATION DES DATES
    twitter_date_format = "EEE MMM dd HH:mm:ss Z yyyy"
    
    df_transformed = df_clean.withColumn(
        "timestamp", 
        F.to_timestamp(F.col("created_at"), twitter_date_format)
    ).withColumn(
        "date_only", 
        F.to_date(F.col("timestamp")).cast("string") # <--- On convertit en String ici !
    )

    # FILTRES
    # On garde les tweets en anglais/français/portugais qui ne sont pas vides
    df_filtered = df_transformed.filter(
        (F.col("text").isNotNull()) & 
        (F.col("lang").isin("en", "fr", "pt"))
    )

    # AGRÉGATION (Exemple : Statistique par utilisateur pour ce fichier)
    df_summary = df_filtered.groupBy("user_handle").agg(
        F.count("tweet_id").alias("nb_tweets"),
        F.first("followers").alias("followers_count"),
        F.collect_list("hashtags").alias("all_hashtags")
    )

    # STOCKAGE DANS MONGODB
    try:
        # On insère les tweets individuels transformés (plus utile pour MongoDB)
        data_to_insert = df_filtered.toPandas().to_dict('records')
        
        if data_to_insert:
            client = pymongo.MongoClient(ATLAS_URI)
            db = client["twitter_db"]
            collection = db["worldcup_tweets"]
            collection.insert_many(data_to_insert)
            print(f"✅ Importé sur Atlas : {os.path.basename(chemin_json)}")
    except Exception as e:
        print(f"⚠️ Erreur Atlas : {e}")

    return df_filtered


# --- LANCEMENT ---
dir_path = os.path.dirname(os.path.realpath(__file__))
dossier = os.path.join(dir_path, "raw")

if os.path.exists(dossier):
    # Nettoyage initial de la collection sur Atlas
    try:
        client = pymongo.MongoClient(ATLAS_URI)
        client["twitter_db"]["worldcup_tweets"].delete_many({})
        print("🧹 Collection Atlas vidée.")
    except Exception as e:
        print(f"⚠️ Erreur de nettoyage : {e}")

    # Récupération de la liste des fichiers .json
    fichiers = [f for f in os.listdir(dossier) if f.endswith('.json') and not f.startswith('.')]
    
    # --- LOAD DES 2285 FICHIERS ---
    nb_test = 2285
    fichiers_test = fichiers[:nb_test]

    def natural_keys(text):
        return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]

    fichiers_test.sort(key=natural_keys) # Tri pour un ordre chronologique
    print(f"🚀 Lancement du test sur {len(fichiers_test)} fichiers...")

    for nom_f in fichiers_test:
        chemin_complet = os.path.join(dossier, nom_f)
        processus_etl_tweets(chemin_complet)
    
    print("\n🏁 Test terminé. Vérifie tes données sur MongoDB Atlas !")
else:
    print(f"❌ Dossier {dossier} introuvable.")