package com.sanjiv.pairrdd;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class JoinPairRdd {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		
		SparkConf conf = new SparkConf().setAppName("JoinPairRdd").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaPairRDD<String, String> students1 = jsc.parallelizePairs(
					Arrays.asList(
							new Tuple2<>("sanjiv", "sanjiv.programmer@gmail.com"),
							new Tuple2<>("aarush", "aarush.singh000@gmail.com")
						)
					);
		JavaPairRDD<String, String> students2 = jsc.parallelizePairs(
				Arrays.asList(
							new Tuple2<>("sanjiv", "patna"),
							new Tuple2<>("kalindi", "Bangalore"),
							new Tuple2<>("pratibha", "sasaram")
						)
				);
		 JavaPairRDD<String, Tuple2<String, String>> studentsInnerJoin = students1.join(students2);
		 studentsInnerJoin.saveAsTextFile("output/innerjoin");
		 
		 JavaPairRDD<String, Tuple2<String, Optional<String>>> studentsLeftOuterJoin = students1.leftOuterJoin(students2); 
		 studentsLeftOuterJoin.saveAsTextFile("output/leftouterjoin");
		 
		 JavaPairRDD<String, Tuple2<Optional<String>, String>> studentsRightOuterJoin = students1.rightOuterJoin(students2);
		 studentsRightOuterJoin.saveAsTextFile("output/rightouterjoin");
		 
		 JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> studentsFullOuterJoin = students1.fullOuterJoin(students2);
		 studentsFullOuterJoin.saveAsTextFile("output/fulluterjoin");
		
		jsc.close();

	}
}
