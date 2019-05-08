package com.sanjiv.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.sanjiv.utils.Util;

public class DatasetAndRdd {

	public static void main(String[] args) {
		
		
		Logger logger = Logger.getLogger(DatasetAndRdd.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		
		SparkConf conf = new SparkConf().setAppName("DatasetAndRdd").setMaster("local[1]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SparkSession session = new SparkSession.Builder().appName("DatasetAndRdd").master("local[1]").getOrCreate();
		
		JavaRDD<String> stackFlowRdd = jsc.textFile("input/2016-stack-overflow-survey-responses.csv");
		JavaRDD<String> filterStackFlowRdd = stackFlowRdd.filter(line -> !line.split(Util.COMMA_DELIMITER, -1)[2].equalsIgnoreCase("country"));
		JavaRDD<StrongDatatypeResponse> selectedRdd = filterStackFlowRdd
				.map(new Function<String, StrongDatatypeResponse>() {
					@Override
					public StrongDatatypeResponse call(String v1) throws Exception {

						String[] spliter = v1.split(Util.COMMA_DELIMITER, -1);
						// split[2] , split[9] , split[6], split[14] ---> country, occupation,
						// ageMidPoint, salaryMidPoint
						
						return new StrongDatatypeResponse(spliter[2], spliter[9],toIntFunction(spliter[6]),
								toIntFunction(spliter[14]) ); 
					}
				});

		Dataset<StrongDatatypeResponse> datasetFromRdd = session.createDataset(selectedRdd.rdd(), Encoders.bean(StrongDatatypeResponse.class));
		
		logger.info("******************Printing Schema************");
		datasetFromRdd.printSchema();
		datasetFromRdd.filter(new FilterFunction<StrongDatatypeResponse>() {
			@Override
			public boolean call(StrongDatatypeResponse value) throws Exception {
				if (value.getAgeMidPoint() == null) {
					return false;
				}
				return value.getAgeMidPoint() > 45 ? true : false;
			}
		}).show(20);
		
		logger.info("******************convert into JavaRdd************");
		JavaRDD<StrongDatatypeResponse> RddFromDataset = datasetFromRdd.toJavaRDD();
		for(StrongDatatypeResponse s : RddFromDataset.take(20)) { 
			System.out.println(s.getCountry() + " --" + s.getOccupation() + "---" + s.getAgeMidPoint() + "---" + s.getSalaryMidPoint());
		}
		
		session.stop();
		jsc.close();
	}
	
	private static Integer toIntFunction(String s) {
		return s.isEmpty() ? null : Math.round(Float.valueOf(s));
	}
}

/*
 * sample output
+-----------+-----------+--------------------+--------------+
|ageMidPoint|    country|          occupation|salaryMidPoint|
+-----------+-----------+--------------------+--------------+
|         65|Afghanistan|                    |          null|
|         55|  Argentina|Developer with a ...|         45000|
|         55|  Australia|Developer with a ...|          null|
|         55|  Australia|     Product manager|        105000|
|         65|  Australia|Full-stack web de...|         55000|
|         65|  Australia|Developer with a ...|         95000|
|         55|  Australia|Full-stack web de...|          5000|
|         65|  Australia|   Desktop developer|        165000|
|         55|    Austria|      Data scientist|          null|
|         55|    Austria|Full-stack web de...|         15000|
|         55|    Austria|Back-end web deve...|          5000|
|         55|    Austria|Full-stack web de...|        125000|
|         65|    Bahamas|   Desktop developer|         95000|
|         55|     Brazil|Full-stack web de...|         15000|
|         55|     Brazil|Full-stack web de...|          null|
|         55|     Brazil|Business intellig...|          null|
|         55|     Brazil|Full-stack web de...|         15000|
|         55|     Brazil|Full-stack web de...|         25000|
|         65|     Brazil|             Analyst|         15000|
|         55|     Brazil|               other|          null|
+-----------+-----------+--------------------+--------------+
only showing top 20 rows
 * 
 */