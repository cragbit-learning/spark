package com.sanjiv.pairrdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(WordCount.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("WordCountByPairRdd").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> regularWordRdd = jsc.textFile("input/word_count.text");
		JavaRDD<String> flattenWordRdd = regularWordRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		// making RegularRDD to pairRDD by using mapToPair function
		JavaPairRDD<String, Integer> tupleWordRdd = flattenWordRdd
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(word, 1);
						return tuple;
					}
				});

		JavaPairRDD<String, Integer> wordsRdd = tupleWordRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		JavaPairRDD<Integer, String> reverseTupleWordRdd = wordsRdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<>(t._2, t._1);
			}
		});
		
		// trying to sort based on key
		JavaPairRDD<Integer, String> sortedWordRdd = reverseTupleWordRdd.sortByKey(false);

		// below one for printing
		for (Tuple2<Integer, String> t : sortedWordRdd.take(5)) { 
			logger.info(t._1 + "---> " + t._2);
		}
		
		logger.info("-------------------------------------------------");
		// you cab print like below one for printing
//		Map<Integer, String>  wordMap = sortedWordRdd.collectAsMap();
//		for(java.util.Map.Entry<String, Integer> w: wordMap.entrySet()) {
//			logger.info(w.getKey() + "---> " + w.getValue());
//			
//		}
		
		jsc.close();

	}

}


class Words implements Serializable{
	
	private static final long serialVersionUID = -3525197547548370698L;
	private String word;
	private Integer count;
	
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	
}





