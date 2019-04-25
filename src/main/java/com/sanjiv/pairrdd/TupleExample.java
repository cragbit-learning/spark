package com.sanjiv.pairrdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TupleExample {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(TupleExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("Tuple").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		Tuple2<Integer, Integer> tupl = new Tuple2<Integer, Integer>(10, 20);
		System.out.println(" here is output : " + tupl._1() + "<<----------->>" + tupl._2());
		System.out.println(" here is output : " + tupl._1 + "<<----------->>" + tupl._2);

		List<Tuple2<String, Integer>> tuple = Arrays.asList(new Tuple2<>("Lily", 23), new Tuple2<>("Jack", 29),
				new Tuple2<>("Mary", 29), new Tuple2<>("James", 8));

		// create RDD from tuple...
		JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(tuple);

		pairRDD.coalesce(1).saveAsTextFile("output/pair_rdd_from_tuple_list");

		// create RDD from regular RDD
		List<String> intString = Arrays.asList("sanjiv 35", "kundan 56", "amit 30", "prakash 45", "aarush 15",
				"rakesh 40");

		JavaRDD<String> regularRdd = jsc.parallelize(intString);

		JavaPairRDD<String, Integer> pairRdd = regularRdd.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] s = t.split(" ");
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(s[0], Integer.parseInt(s[1]));
				return tuple;
			}
		});

		pairRdd.coalesce(1).saveAsTextFile("output/pair_rdd_from_tuple_list1");
		
		jsc.close();

	}

}
