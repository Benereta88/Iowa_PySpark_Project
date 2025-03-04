from pymongo import MongoClient
import certifi
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Anslut till MongoDB med certifi:s certifikat för SSL-verifiering.
client = MongoClient(
    "mongodb+srv://beneretahoxha8:Pipiina-18@cluster0.jwm6a.mongodb.net/test?ssl=true&authSource=admin",
    ssl=True,
    tlsCAFile=certifi.where()  # Använd certifi:s certifikat för SSL-verifiering
)

# Välj databasen och collection, om de inte finns skapas de när du kör koden.
database = client["spark_test"]
collection = database["python_test"]

# Skapar en Spark-session.
spark = (SparkSession.builder.appName("Name-our-app")
         .config("spark.memory.offHeap.enabled", "true")
         .config("spark.memory.offHeap.size", "10g")
         .getOrCreate())

# Läs in CSV-filen i en Spark DataFrame
dataframe = spark.read.csv('/Users/benereta/PycharmProjects/Iowa project/Iowa_Vehicle_Crashes_sample (1).csv', header=True, escape="\"")

# Kör en aggregering på DataFrame för att få antalet unika 'Iowa DOT Case Number' per 'Drug or Alcohol'
result = dataframe.groupBy('Drug or Alcohol ').agg(countDistinct('Iowa DOT Case Number').alias('amount'))

# Visa resultatet i terminalen
result.show()

# Konvertera Spark DataFrame till Pandas DataFrame
pandas_df = result.toPandas()

# Konvertera Pandas DataFrame till en lista av dictionaries (JSON-format)
data_dict = pandas_df.to_dict("records")

# Skriv ut den konverterade datan
print(data_dict)

# Skicka data till MongoDB
try:
    collection.insert_many(data_dict)
    print("Data has been successfully inserted into MongoDB!")
except Exception as e:
    print(f"An error occurred while inserting data into MongoDB: {e}")
print("Data har skickats till MongoDB Atlas!")