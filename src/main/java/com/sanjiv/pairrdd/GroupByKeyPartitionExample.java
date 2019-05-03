package com.sanjiv.pairrdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import com.sanjiv.utils.Util;

import scala.Tuple2;

public class GroupByKeyPartitionExample {

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
	
	/*GroupByKey is very costly operation It shuffle the data across the network 
	 * there is method called partitonBy, which can make sure like data 
	 * with same key appear on the same node.
	 * It can help lots of transformation like groupByKey,reduceByKey,combinedByKey,join,leftOuterJoin,rightOuterJoin and lookup and so many..
	 * Few transformation also affect like Map, flatMap and many more...
	 */
	
	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(GroupByKeyPartitionExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("GroupByKeyPartitionExample").setMaster("local");
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
		
		//using partitonBy to make sure same key go to on same node.
		JavaPairRDD<String, String> airportPairRddWithPartitions = airportPairRdd.partitionBy(new HashPartitioner(4));
		// saving into memory otherwise it has to re-shuffle again.
		JavaPairRDD<String, String> airportPairRddWithPartitionsInMemory =  airportPairRddWithPartitions.persist(StorageLevel.MEMORY_ONLY());
		
		JavaPairRDD<String, Iterable<String>> groupAirportRdd = airportPairRddWithPartitionsInMemory.groupByKey();
		
		//sort by key
		JavaPairRDD<String, Iterable<String>> sortedGroupByAirportRdd = groupAirportRdd.sortByKey(true);
		
		for (Tuple2<String, Iterable<String>> s : sortedGroupByAirportRdd.collect()) {
			logger.info(s._1 + "<<<----->>" + s._2); 
		}
		jsc.close();
	}

}
