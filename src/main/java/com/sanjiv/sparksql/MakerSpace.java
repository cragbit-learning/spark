package com.sanjiv.sparksql;

import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MakerSpace {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(HousePriceProblem.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkSession session = new SparkSession.Builder().appName("MakerSpace").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> makerSpaceRecords = dataFrameReader.option("header", true)
				.csv("input/uk-makerspaces-identifiable-data.csv");
		Dataset<Row> postCodeRecords = dataFrameReader.option("header", true).csv("input/uk-postcode.csv");
		Dataset<Row> postCodeRecordsWithSpace = postCodeRecords.withColumn("Postcode",
				concat_ws("", col("Postcode"), lit(" ")));

		makerSpaceRecords.join(postCodeRecordsWithSpace,
				makerSpaceRecords.col("Postcode").startsWith(postCodeRecordsWithSpace.col("Postcode")), "LEFT_OUTER")
				.groupBy("Region").count().orderBy(col("count").desc()).show();

		/*  sample output
		 *    
+-----------------+-----+
|           Region|count|
+-----------------+-----+
|       Manchester|    3|
|          Cardiff|    3|
|          Glasgow|    3|
|    Tower Hamlets|    3|
|          Bristol|    2|
|        Sheffield|    2|
|        Southwark|    2|
|          Belfast|    2|
|           Oxford|    2|
|         Aberdeen|    2|
|        Liverpool|    2|
|Brighton and Hove|    2|
|             null|    2|
|          Lambeth|    2|
|           Camden|    2|
|            Leeds|    2|
|         Coventry|    1|
|       Sunderland|    1|
|       Wandsworth|    1|
|       Eastbourne|    1|
+-----------------+-----+
only showing top 20 rows
		 * 
		 */
	}

}
