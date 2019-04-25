package com.sanjiv.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

public class ReduceTakeCountByValueCollect {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(ReduceTakeCountByValueCollect.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("airport").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(2, 4, 6, 8, 9, 1, 0, 4, 3,3,5,6,0,3);
		
		JavaRDD<Integer> numberRdd = jsc.parallelize(numbers);
		
		numberRdd.persist(StorageLevel.MEMORY_ONLY()); // caching the RDD if It require the same RDD for multiple times.

		List<Integer> takeInteger = numberRdd.take(3);

		for (Integer num : takeInteger) {
			logger.info(num);
		}

//		JavaRDD<String> linesRdd = jsc.textFile("input/word_count.text");
//		List<String> takeLines = linesRdd.take(3);
//		for (String line : takeLines) {
//			logger.info(line);
//		}

		/*
		 * CountByValue example
		 * 
		 */
		Map<Integer,Long> numberCount = numberRdd.countByValue();
		for(Map.Entry<Integer, Long> number: numberCount.entrySet()) {
			logger.info(number.getKey() + " = " + number.getValue());
		}
			
		/*
		 * Reduce Example
		 * 
		 */
		
		Integer sum = numberRdd.reduce(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		logger.info("SUM : " + sum);
		
		//or you can write like below
		Integer sumNumber = numberRdd.reduce((n1,n2) -> (n1+n2) );
		logger.info("SUM : " + sumNumber);
		
		/*
		 * Collect Example
		 * 
		 */
		
		List<Integer> nums = numberRdd.collect();
		for (Integer num : nums) {
			logger.info(num);
		}
		
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		jsc.close();
	}
	

}
