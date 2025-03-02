# Importera nödvändiga bibliotek
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Skapa en Spark-session
spark = SparkSession.builder \
    .appName("Iowa2025") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "10g") \
    .config('spark.sql.catalogImplementation', 'hive') \
    .config('spark.sql.legacy.createHiveTableByDefault', 'false') \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Läs in trafikolycksdata
crashes = spark.read.csv("/Users/benereta/PycharmProjects/Iowa project/Iowa_Vehicle_Crashes_sample (1).csv",
                         header=True, inferSchema=True)
crashes = crashes.withColumn("Date of Crash", to_date(crashes["Date of Crash"], "MM/dd/yyyy"))

# Kontrollera datans struktur
print("Step 1: Crashes Data")
crashes.printSchema()
crashes.show(5)
print("Antal rader i Crashes:", crashes.count())

# Skapa temporär vy för SQL-frågor
crashes.createOrReplaceTempView("crashes")

# Läs in spritförsäljningsdata
liquorSales = spark.read.csv("/Users/benereta/PycharmProjects/Iowa project/Iowa_Liquor_Sales_sample.csv",
                             header=True, inferSchema=True)
liquorSales = liquorSales.withColumn("Date", to_date(liquorSales["Date"], "MM/dd/yyyy"))

# Kontrollera datans struktur
print("Step 2: Liquor Sales Data")
liquorSales.printSchema()
liquorSales.show(5)
print("Antal rader i Liquor Sales:", liquorSales.count())

# Skapa temporär vy för SQL-frågor
liquorSales.createOrReplaceTempView("liquorSales")

# Steg 4: SQL-fråga för att slå ihop data
liquorAndCrashes = spark.sql("""
    SELECT 
        ROUND(SUM(l.`Volume Sold (liters)`)) AS liquorSold,  
        COUNT(c.`Date of Crash`) AS Crashes,  -- Ändrat från SUM(c.`Crash Severity`) till COUNT   
        date_format(l.`Date`, 'yyyyMM') AS monthOfYear       
    FROM 
        liquorSales l
    INNER JOIN 
        crashes c
    ON 
        date_format(l.`Date`, 'yyyyMM') = date_format(c.`Date of Crash`, 'yyyyMM')
    WHERE 
        date_format(l.`Date`, 'yyyy') BETWEEN '2014' AND '2016'  
    GROUP BY 
        date_format(l.`Date`, 'yyyyMM')
    ORDER BY 
        date_format(l.`Date`, 'yyyyMM')
""")

# Visa aggregerade resultat
print("Step 3: Sammanfogade data")
liquorAndCrashes.show()

# Konvertera till Pandas DataFrame för visualisering
liquorAndCrashesData = liquorAndCrashes.toPandas()

# Plotta data
plt.figure(figsize=(12, 6))
plt.plot(
    liquorAndCrashesData['monthOfYear'],
    liquorAndCrashesData['liquorSold'],
    marker='o', linestyle='-', color='b', label='Sprit såld (L)'
)
plt.plot(
    liquorAndCrashesData['monthOfYear'],
    liquorAndCrashesData['Crashes'],
    marker='o', linestyle='-', color='r', label='Olyckor'
)
plt.title('Spritförsäljning vs Trafikolyckor per Månad (2014-2016)')
plt.xlabel('Månad (yyyyMM)')
plt.ylabel('Antal')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Steg 5: Regressionsanalys
# Omvandla data för ML-modellering
assembler = VectorAssembler(inputCols=['liquorSold'], outputCol='features')
output = assembler.transform(liquorAndCrashes)
final_data = output.select('features', 'Crashes')

# Dela upp i tränings- och testdata
train_data, test_data = final_data.randomSplit([0.7, 0.3])

# Skapa en linjär regressionsmodell
lr = LinearRegression(labelCol='Crashes')

# Träna modellen
lrModel = lr.fit(train_data)

# Visa regressionskoefficienter och intercept
print("Koefficienter: {} Intercept: {}".format(lrModel.coefficients, lrModel.intercept))

# Visa modellens prestanda
trainingSummary = lrModel.summary
print("Antal iterationer: %d" % trainingSummary.totalIterations)
print("Rötmedelkvadratfel (RMSE): %f" % trainingSummary.rootMeanSquaredError)
print("R²-värde: %f" % trainingSummary.r2)

# Evaluera på testdata
test_results = lrModel.evaluate(test_data)

# Visa residualer
print("Residualer (Test Data)")
test_results.residuals.show()

# Prediktioner
predictions = lrModel.transform(test_data)
print("Prediktioner")
predictions.show()

# Visualisera prediktioner
actual_crashes = [row['Crashes'] for row in predictions.select("Crashes").collect()]
predicted_crashes = [row['prediction'] for row in predictions.select("prediction").collect()]

plt.figure(figsize=(12, 6))
plt.scatter(actual_crashes, predicted_crashes, color='blue', alpha=0.5)
plt.xlabel('Faktiska olyckor')
plt.ylabel('Predikterade olyckor')
plt.title('Faktiska vs Predikterade Olyckor')
plt.grid(True)
plt.show()

