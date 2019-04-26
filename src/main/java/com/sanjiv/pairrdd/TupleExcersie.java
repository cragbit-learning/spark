package com.sanjiv.pairrdd;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TupleExcersie {

	public static void main(String[] args) {

		/*
		 * profileId name email phone viewesProfileList
		 * 
		 * 100 sanjiv sanjiv.programmer@gmail.com 9741019226 201,208,5006,1004
		 * 
		 * 200 ashok ashok.programmer@gmail.com 9745619226 2091,2208,2006,1004
		 * 
		 * 201 aarush sanjiv.programmer@gmail.com 7474747477 201,208,5006,100
		 * 
		 * 10004 Rajeev aarush.programmer@gmail.com 8726253535 201,208,5006,1004
		 * 
		 * 101 Raman Raman.programmer@gmail.com 7725353535 100,208,5006,1004
		 * 
		 * 104 Sarkar Sarkar.programmer@gmail.com 8272626363 201,208,5006,1004
		 * 
		 * 2001 Arnab Arnab.programmer@gmail.com 6747644764 201,208,5006,1004
		 * 
		 * 4000 Nikhil Nikhil.programmer@gmail.com 6747689999 201,100,5006,1004
		 * 
		 * 
		 */

		Logger logger = Logger.getLogger(TupleExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("TupleExcersice").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> profileRdd = jsc.textFile("input/profile.text");

		JavaRDD<String> custProfileRdd = profileRdd.map(line -> {
			String[] profile = line.split("\t");
			return StringUtils.join(new String[] { profile[0], profile[4] }, "-");
		});

		JavaPairRDD<String, List<String>> customTupleProfile = custProfileRdd
				.mapToPair(new PairFunction<String, String, List<String>>() {
					@Override
					public Tuple2<String, List<String>> call(String t) throws Exception {

						String[] profileAttibute = t.split("-");
						Tuple2<String, List<String>> tuple = new Tuple2<String, List<String>>(profileAttibute[0],
								Arrays.asList(profileAttibute[1].split(",")));

						return tuple;
					}
				});

		//applying filter to get the only those records who see the profile_id=100
		
		// JavaPairRDD<String, List<String>> filteredProfile =
		// customTupleProfile.filter(profile -> profile._2.contains("100"));

		//another solution
		
		JavaPairRDD<String, String> filteredProfile = customTupleProfile
				.mapValues(new Function<List<String>, String>() {

					@Override
					public String call(List<String> v1) throws Exception {
						if (v1.contains("100"))
							return "100";
						else
							return null;
					}
				}).filter(filterProfileValue -> filterProfileValue._2 != null);

		filteredProfile.coalesce(1).saveAsTextFile("output/profile");

		jsc.close();

	}

}
