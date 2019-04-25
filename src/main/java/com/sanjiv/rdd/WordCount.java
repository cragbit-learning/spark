package com.sanjiv.rdd;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(WordCount.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[3]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> linesRdd = jsc.textFile("input/word_count.text");
		JavaRDD<String> flatternWordRdd = linesRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		
		Map<String, Long> words = flatternWordRdd.countByValue();

		for (java.util.Map.Entry<String, Long> word : words.entrySet()) {
			logger.info(word.getKey() + " = " + word.getValue());
		}
		jsc.close();
	}

}
