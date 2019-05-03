package com.sanjiv.accumulator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.sanjiv.utils.Util;

import scala.Option;

public class Accumulators {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		
		SparkConf conf = new SparkConf().setAppName("accumulators").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		// declaring accumulator variables
		final LongAccumulator totalRecords = new LongAccumulator();
		final LongAccumulator missingSalary = new LongAccumulator();

		// register accumulator variables
		totalRecords.register(sc, Option.apply("total"), false);
		missingSalary.register(sc, Option.apply("missingSalary"), false);

		// now load the input file.
		JavaRDD<String> stackSalaryRdd = jsc.textFile("input/2016-stack-overflow-survey-responses.csv");

		// trying to find out Indian salary
		JavaRDD<String> filteredCanadaSalary = stackSalaryRdd.filter(record -> {
			totalRecords.add(1);
			if ((record.split(Util.COMMA_DELIMITER,-1)[14].isEmpty())) {
				missingSalary.add(1);
			}
			return record.split(Util.COMMA_DELIMITER,-1)[2].equalsIgnoreCase("Canada");
		});
		
		filteredCanadaSalary.saveAsTextFile("output/stackoverflow"); 
		
		System.out.println("Total records : " + totalRecords.value()); 
		System.out.println("Total records from Canada : " + filteredCanadaSalary.count());
		System.out.println("Total missing salary : " + missingSalary.value());

		jsc.close();
	}

}
