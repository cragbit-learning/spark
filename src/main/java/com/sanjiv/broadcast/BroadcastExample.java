package com.sanjiv.broadcast;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.sanjiv.utils.Util;

import scala.Tuple2;

public class BroadcastExample {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(BroadcastExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("broadcastExample").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		Broadcast<Map<String, String>> postcodeData = jsc.broadcast(loadPostCodeData());
		JavaRDD<String> makerspaceRdd = jsc.textFile("input/uk-makerspaces-identifiable-data.csv");

		JavaRDD<String> filterMakerspaceRdd = makerspaceRdd.filter(line -> !line.contains("Timestamp"));
		JavaRDD<String> postCodeandName = filterMakerspaceRdd.map(line -> {
			String[] words = line.split(Util.COMMA_DELIMITER, -1);
			if (words[4].isEmpty()) {
				return "unknown";
			}
			String[] postCode = words[4].trim().split(" ");
			if (!postcodeData.getValue().containsKey(postCode[0])) {
				return "unknown";
			}
			return StringUtils.join(new String[] { postcodeData.getValue().get(postCode[0]), postCode[0] }, ",");
		});

		JavaPairRDD<String, Integer> regionWithMakeshopValue = postCodeandName
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {

						String[] words = t.split(Util.COMMA_DELIMITER, -1);
						Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(words[0], 1);
						return tuple;
					}
				});

		JavaPairRDD<String, Integer> regionAndNumberOfShop = regionWithMakeshopValue
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		for (Map.Entry<String, Integer> r : regionAndNumberOfShop.collectAsMap().entrySet()) {

			System.out.println(r.getKey() + " = " + r.getValue());
		}

		jsc.close();
	}

	private static Map<String, String> loadPostCodeData() {

		BufferedReader br = null;
		Map<String, String> postcodeRegion = new HashMap<>();
		try {
			br = new BufferedReader(new FileReader("input/uk-postcode.csv"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			String line = null;
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}

			while (line != null) {
				String[] words = line.split(Util.COMMA_DELIMITER, -1);
				postcodeRegion.put(words[0].trim(), words[7].trim());
				try {
					line = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return postcodeRegion;
	}

}
