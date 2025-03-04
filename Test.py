# Importing Libraries
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("Iowa2025").config("spark.memory.offHeap.enabled", "true").config(
    "spark.memory.offHeap.size", "10g").config('spark.sql.catalogImplementation', 'hive').config('spark.sql.legacy.createHiveTableByDefault', 'false').getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Reading the data
crashes = spark.read.csv("/Users/benereta/PycharmProjects/Iowa project/Iowa_Vehicle_Crashes_sample (1).csv", header=True, inferSchema=True)
crashes = crashes.withColumn("Date of Crash", to_date(crashes["Date of Crash"], "MM/dd/yyyy"))
print("Step 1: Crashes Data")
crashes.show(5)
crashes.createOrReplaceTempView("crashes")
print("Crashes Data")
spark.sql("SELECT * FROM crashes LIMIT 5").show()

# Reading the data
liquorSales = spark.read.csv("/Users/benereta/PycharmProjects/Iowa project/Iowa_Liquor_Sales_sample.csv", header=True, inferSchema=True)
liquorSales = liquorSales.withColumn("Date", to_date(liquorSales["Date"], "MM/dd/yyyy"))
liquorSales.createOrReplaceTempView("liquorSales")
print("Step 2: Liquor Sales Data")
liquorSales.show(5)

# Step 4: SQL Query to merge data
# SQL Query to merge data
liquorAndCrashes = spark.sql("""
    SELECT 
        ROUND(SUM(l.`Volume Sold (liters)`)) AS liquorSold,  
        SUM(c.`Crash Severity`) AS Crashes,                 
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

# Show aggregated results
liquorAndCrashes.show()

# Convert to Pandas DataFrame
liquorAndCrashesData = liquorAndCrashes.toPandas()

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(
    liquorAndCrashesData['monthOfYear'],
    liquorAndCrashesData['liquorSold'],
    marker='o', linestyle='-', color='b', label='Liquor sold (L)'
)
plt.plot(
    liquorAndCrashesData['monthOfYear'],
    liquorAndCrashesData['Crashes'],
    marker='o', linestyle='-', color='r', label='Crashes'
)
plt.title('Liquor Sold vs Car Crashes per Month (2014-2016)')
plt.xlabel('Month (yyyyMM)')
plt.ylabel('Amount')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Step 5: Regression Analysis
# Regression Analysis
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# Prepare the data
assembler = VectorAssembler(inputCols=['liquorSold'], outputCol='features')
output = assembler.transform(liquorAndCrashes)
final_data = output.select('features', 'Crashes')


# Split the data
train_data, test_data = final_data.randomSplit([0.7, 0.3])

# Create a Linear Regression Model
lr = LinearRegression(labelCol='Crashes')

# Fit the model
lrModel = lr.fit(train_data)

# Print the coefficients and intercept for linear regression
print("Coefficients: {} Intercept: {}".format(lrModel.coefficients, lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RISE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# Visualize the results
test_results = lrModel.evaluate(test_data)
test_results.residuals.show()
print("RISE on test data: %f" % test_results.rootMeanSquaredError)

# Predictions
predictions = lrModel.transform(test_data)
predictions.show()

# Visualisation med graffigur
plt.figure(figsize=(12, 6))
plt.scatter(test_results.predictions.select('Crashes').collect(), test_results.predictions.select('prediction').collect())
plt.xlabel('Crashes')
plt.ylabel('Predicted Crashes')
plt.title('Crashes vs Predicted Crashes')
plt.grid(True)
plt.show()

