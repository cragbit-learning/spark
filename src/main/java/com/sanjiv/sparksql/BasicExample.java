package com.sanjiv.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class BasicExample {

	public static void main(String[] args) {
		
		
		Logger logger = Logger.getLogger(BasicExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		
		SparkSession session = SparkSession.builder().appName("BasicSparkSql").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> records = dataFrameReader.option("header", "true").csv("input/2016-stack-overflow-survey-responses.csv");
		
		logger.info("---------------printing schena----------");
		records.printSchema();
		
		logger.info("---------------showin records----------");
		records.show(20);
		
		logger.info("---------------select specific column----------");
		records.select("country", "age_midpoint", "gender").show(10);
		
		logger.info("---------------filter specific column----------");
		records.filter(col("country").equalTo("Afghanistan")).show();
		
		

	}

}
