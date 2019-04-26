package com.sanjiv.pairrdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.esotericsoftware.minlog.Log;
import com.sanjiv.utils.Util;

import scala.Tuple2;

public class GroupByKeyExample {

	/* Create a Spark program to read the airport data from in/airports.text,
    output the the list of the names of the airports located in each country.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
    "Papua New Guinea",  ["Goroka", "Madang", ...]
    ...
  */
	
	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(GroupByKeyExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("GroupByKeyExample").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> regularAirportRdd = jsc.textFile("input/airports.text");
		 JavaPairRDD<String, String> airportPairRdd= regularAirportRdd.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 5639334691467506107L;
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String [] line = t.split(Util.COMMA_DELIMITER);
				return new Tuple2<String, String>(line[3], line[1]);
			}
		});
		JavaPairRDD<String, Iterable<String>> groupAirportRdd = airportPairRdd.groupByKey();
		
		
		for (Tuple2<String, Iterable<String>> s : groupAirportRdd.collect()) {
			logger.info(s._1 + "<<<----->>" + s._2); 
		}
		
		jsc.close();

	}

}
