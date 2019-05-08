package com.sanjiv.sparksql;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StronglyTypeDataset {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(StronglyTypeDataset.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkSession session = new SparkSession.Builder().appName("StronglyTypeDataset").master("local").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> records = dataFrameReader.option("header", true)
				.csv("input/2016-stack-overflow-survey-responses.csv");
		Dataset<Row> selectedRecords = records.select(
				col("country"), 
				col("occupation"), 
				col("age_midpoint").as("ageMidPoint").cast("integer"),
				col("salary_midpoint").as("salaryMidPoint").cast("integer")
		);
		
		Dataset<StrongDatatypeResponse> strongDataypeRecords = selectedRecords
				.as(Encoders.bean(StrongDatatypeResponse.class));

		logger.info("*****************selected Schema***********************");
		strongDataypeRecords.printSchema();

		logger.info("*****************showing Records***********************");
		strongDataypeRecords.show();

		logger.info("*****************filter Schema***********************");
		// strongDataypeRecords.filter(col("ageMidPoint").geq(22)).show();

		strongDataypeRecords.filter(new FilterFunction<StrongDatatypeResponse>() {
			@Override
			public boolean call(StrongDatatypeResponse value) throws Exception {
				if(value.getAgeMidPoint() == null) return false;
				
				if (value.getAgeMidPoint() >= 22)
					return true;
				else
					return false;
			}
		}).show();
		
		
		strongDataypeRecords.filter(new FilterFunction<StrongDatatypeResponse>() {
			@Override
			public boolean call(StrongDatatypeResponse value) throws Exception {
				if (value.getCountry().equalsIgnoreCase("Albania"))
					return true;
				else
					return false;
			}
		}).orderBy(col("ageMidPoint").$greater(22).desc()).show();
		
		session.stop();
	}
	
	
	/*
	 * sample output
	 * 
 +-------+--------------------+-----------+--------------+
|country|          occupation|ageMidPoint|salaryMidPoint|
+-------+--------------------+-----------+--------------+
|Albania|                null|         27|          null|
|Albania|Back-end web deve...|         27|          5000|
|Albania|Full-stack web de...|         27|          5000|
|Albania|Full-stack web de...|         27|          5000|
|Albania|Back-end web deve...|         22|          5000|
|Albania|Full-stack web de...|         22|         15000|
|Albania|Back-end web deve...|         22|         15000|
+-------+--------------------+-----------+--------------+
	 * 
	 */

}
