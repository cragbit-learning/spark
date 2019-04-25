package com.sanjiv.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(Union.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("NasaLogUnion").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> julyLogRdd = jsc.textFile("input/nasa_19950701.tsv");
		JavaRDD<String> augLogRdd  = jsc.textFile("input/nasa_19950801.tsv");
	
		JavaRDD<String> combinedLog = julyLogRdd.union(augLogRdd);
		
		JavaRDD<String> filteredLog = combinedLog.filter(line -> excludeHeader(line));
		
		logger.info("Total Log count RDD : " + filteredLog.count()); 
		
		JavaRDD<String> sampleLog = filteredLog.sample(true, 0.1);
		
		logger.info("Sample Log count RDD : " + sampleLog.count()); 
		
		sampleLog.saveAsTextFile("output/nasa_log");
		
		jsc.close();
	}

	private static boolean excludeHeader(String line) { 
		return (!line.startsWith("host") && !line.contains("bytes")); 
	}

}
