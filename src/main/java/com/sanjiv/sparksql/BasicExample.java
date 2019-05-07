package com.sanjiv.sparksql;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BasicExample {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(BasicExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkSession session = SparkSession.builder().appName("BasicSparkSql").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> records = dataFrameReader.option("header", "true")
				.csv("input/2016-stack-overflow-survey-responses.csv");

		logger.info("---------------printing schena----------");
		records.printSchema();

		logger.info("---------------showin records----------");
		records.show(20);

		logger.info("---------------select specific column----------");
		records.select("country", "age_midpoint", "gender").show(10);

		logger.info("---------------filter specific column----------");
		records.filter(col("country").equalTo("Afghanistan")).show();

		logger.info("---------------group by column----------");
		records.groupBy((col("country"))).count().orderBy(col("count").desc()).show();

		logger.info("---------------changing the data type of column----------");
		Dataset<Row> castDataType = records.withColumn("salary_midpoint", col("salary_midpoint").cast("integer"))
				.withColumn("age_midpoint", col("age_midpoint").cast("integer"));
		castDataType.printSchema();
		
		logger.info("---------------filter based on age_midpoint--------------");
		castDataType.filter(col("age_midpoint").$less(20)).show(); 
		

		session.stop();

	}

}
