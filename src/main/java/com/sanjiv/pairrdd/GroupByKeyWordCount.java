package com.sanjiv.pairrdd;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class GroupByKeyWordCount {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(GroupByKeyExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("GroupByKeyWordCount").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("sanjiv", "kumar", "ashok", "kumar", "gupta", "singh", "sanjiv", "ashok",
				"aarush", "kalindi");

		JavaRDD<String> namesRdd = jsc.parallelize(names);
		JavaPairRDD<String, Integer> namesPairRdd= namesRdd.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairRDD<String, Iterable<Integer>> gruopByKeyNames = namesPairRdd.groupByKey();
		
		
		Map<String, Integer> wordCount = gruopByKeyNames.mapValues(new Function<Iterable<Integer>, Integer>() {
			@Override
			public Integer call(Iterable<Integer> v1) throws Exception {
				return Iterables.size(v1);
			}
		}).collectAsMap();
		
		for(Map.Entry<String, Integer> w : wordCount.entrySet()) {
			logger.info(w.getKey() + "--->" + w.getValue()); 
		}
		
		logger.info("******************************************************");
		
		// trying through reduceByKey
		Map<String, Integer> wordCounts = namesPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).collectAsMap();
		
		for(Map.Entry<String, Integer> w : wordCounts.entrySet()) {
			logger.info(w.getKey() + "--->" + w.getValue()); 
		}
		
		jsc.close();
	}

}
