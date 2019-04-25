package com.sanjiv.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(final String[] args) throws InterruptedException {
		SparkConf sc = new SparkConf()
		        .setMaster("local")
		        .setAppName("MongoSparkConnectorTour")
		        .set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb")
		        .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb")
		 		.set("spark.mongodb.output.database", "sanjiv")
		 		.set("spark.mongodb.output.collection", "mycollection");

		JavaSparkContext jsc = new JavaSparkContext(sc); // Create a Java Spark Context
	
		// Create a RDD of 10 documents
	    JavaRDD<Document> documents = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
	            (new Function<Integer, Document>() {
	      public Document call(final Integer i) throws Exception {
	          return Document.parse("{test: " + i + "}");
	      }
	    });

	    /*Start Example: Save data from RDD to MongoDB*****************/
	    MongoSpark.save(documents);
	    /*End Example**************************************************/
		
	  }
}
