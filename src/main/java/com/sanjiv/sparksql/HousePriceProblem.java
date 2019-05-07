package com.sanjiv.sparksql;


import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceProblem {
	
	/* Create a Spark program to read the house data from in/RealEstate.csv,
    group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.
    Sample output:
        +----------------+-----------------+----------+
        |        Location| avg(Price SQ Ft)|max(Price)|
        +----------------+-----------------+----------+
        |          Oceano|           1145.0|   1195000|
        |         Bradley|            606.0|   1600000|
        | San Luis Obispo|            459.0|   2369000|
        |      Santa Ynez|            391.4|   1395000|
        |         Cayucos|            387.0|   1500000|
        |.............................................|
        |.............................................|
        |.............................................|
     */

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(HousePriceProblem.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkSession session = SparkSession.builder().appName("HousePriceProblem").master("local").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> records = dataFrameReader.option("header", true).csv("input/RealEstate.csv");

		logger.info("##########Printing schema ##################");
		records.printSchema();

		logger.info("##########changing data schema ##################");
		Dataset<Row> recordsChangedDataType = records.withColumn("Price", col("Price").cast("long"));
		Dataset<Row> recordsNotNull = recordsChangedDataType.filter(col("Price").isNotNull());
		
		logger.info("##########printing final result ##################");
		recordsNotNull.groupBy("Location").agg(avg(col("Price")), max("Price SQ Ft"), min("Price SQ Ft"), max("Price"))
				.orderBy(max("Price").desc()).show();
		
		session.stop();
		
		/* sample output
		 * 
+----------------+------------------+----------------+----------------+----------+
|        Location|        avg(Price)|max(Price SQ Ft)|min(Price SQ Ft)|max(Price)|
+----------------+------------------+----------------+----------------+----------+
|   Arroyo Grande|1013958.3333333334|           87.34|         1086.76|   5499000|
|         Cambria|1076333.3333333333|          812.98|          269.49|   2995000|
| San Luis Obispo|1444666.6666666667|          567.56|          343.70|   2369000|
|     Avila Beach|1205666.6666666667|          686.02|          376.67|   1999000|
|   Arroyo Grande| 537023.2142857143|          351.14|          152.25|   1900000|
|     Pismo Beach| 772374.5833333334|          819.40|          295.48|   1799000|
|          Nipomo| 430629.4117647059|           95.78|          111.78|   1700000|
|         Bradley|         1600000.0|          606.06|          606.06|   1600000|
|         Cayucos|         1500000.0|          386.60|          386.60|   1500000|
|       Templeton| 705890.9090909091|          341.39|          169.73|   1399000|
|      Santa Ynez|          881800.0|          518.01|          300.69|   1395000|
|        Los Osos|          531000.0|          539.57|          103.47|   1350000|
|          Oceano|          392640.0|          483.37|          108.83|   1250000|
|        Los Osos| 359704.7619047619|          397.44|          147.69|   1249000|
|          Oceano|         1195000.0|         1144.64|         1144.64|   1195000|
|     Out Of Area|         1195000.0|          314.47|          314.47|   1195000|
|       Morro Bay| 689223.0769230769|          499.35|          227.53|   1100000|
|          Nipomo| 454166.6666666667|           98.16|          151.91|   1065000|
|    Grover Beach|          684000.0|          341.67|          275.89|    999000|
|      Atascadero|          477950.0|          562.50|          116.33|    995000|
+----------------+------------------+----------------+----------------+----------+
only showing top 20 rows
		 * 
		 * 
		 */	
	}

}
