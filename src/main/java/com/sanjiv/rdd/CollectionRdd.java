package com.sanjiv.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectionRdd {

	public static void main(String[] args) {
		
		
		Logger logger  = Logger.getLogger(CollectionRdd.class);
		System.setProperty("hadoop.home.dir","C:\\hadoop" );
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("collectionRdd").setMaster("local[3]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		
		
		List<String> names = Arrays.asList("saniv","kumar","singh","aarush","kalindi","pratibha");
		JavaRDD<String> namesRdd = jsc.parallelize(names);
		logger.info("Total number of elements : " + namesRdd.count());
		
		List<String> nameList = namesRdd.collect();
		//System.out.println("Names : " + nameList); 
		logger.info("Names : " + nameList);
		
		/* below code halting main thread to check the UI on http://localhost:4040 */
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		jsc.close();
	}

}
