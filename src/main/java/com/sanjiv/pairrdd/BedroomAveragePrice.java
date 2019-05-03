package com.sanjiv.pairrdd;

import java.io.Serializable;
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

import scala.Tuple2;

public class BedroomAveragePrice {

	/*
	 * Create a Spark program to read the house data from input/RealEstate.csv, output
	 * the average price for houses with different number of bedrooms.
	 * Sample output
	 * (3, 325000) 
	 * (1, 266356) 
	 * (2, 325000) 
	 * 3, 1 and 2 mean the number of bedrooms. 325000 means the average price of
	 * houses with 3 bedrooms is 325000.
	 */
/*	
	MLS		Location			Price	Bedrooms	Bathrooms	Size	Price_SQ_Ft	Status
	132842	Arroyo Grande		795000	3			3			2371	335.3		Short Sale
	134364	Paso Robles			399000	4			3			2818	141.59		Short Sale
	135141	Paso Robles			545000	4			3			3032	179.75		Short Sale
	135712	Morro Bay			909000	4			4			3540	256.78		Short Sale
	136282	Santa Maria-Orcutt	109900	3			1			1249	87.99		Short Sale
	136431	Oceano				324900	3			3			1800	180.5		Short Sale
*/

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(TupleExample.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("BedroomAveragePrice").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> regularRealStateRdd = jsc.textFile("input/RealEstate.csv");

		//removing header
		JavaRDD<String> filterregularRealStateRdd = regularRealStateRdd.filter(line -> !line.contains("Bedrooms"));

		JavaPairRDD<Integer, Bedroom> realStatePairRdd = filterregularRealStateRdd
				.mapToPair(new PairFunction<String, Integer, Bedroom>() {
					
					@Override
					public Tuple2<Integer, Bedroom> call(String realState) throws Exception {
						String[] realStateDetail = realState.split(",");
						Bedroom bedroom = new Bedroom();
						bedroom.setType(Integer.parseInt(realStateDetail[3]));
						bedroom.setPrice(Double.parseDouble(realStateDetail[2]));
						bedroom.setCount(1);
						return new Tuple2<Integer, Bedroom>(Integer.parseInt(realStateDetail[3]), bedroom);
					}
				});

		JavaPairRDD<Integer, Bedroom> bedroomTypePairRdd = realStatePairRdd
				.reduceByKey(new Function2<Bedroom, Bedroom, Bedroom>() {
					
					@Override
					public Bedroom call(Bedroom v1, Bedroom v2) throws Exception {
						Bedroom bedroom = new Bedroom();
						bedroom.setCount(v1.getCount() + v2.getCount());
						bedroom.setPrice(v1.getPrice() + v2.getPrice());
						return bedroom;
					}
				});

		JavaPairRDD<Integer, Bedroom> rooms = bedroomTypePairRdd.mapValues(new Function<Bedroom, Bedroom>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Bedroom call(Bedroom v1) throws Exception {
				Bedroom bedroom =  new Bedroom();
				bedroom.setAvgPrice( v1.getPrice() / v1.getCount()); 
				bedroom.setCount(v1.getCount());
				return bedroom;
			}
		});
		
		// implementing sorting 
//		JavaPairRDD<Integer, Bedroom> sortRoomByType = rooms.sortByKey(new Comparator<Integer>() {
//			
//			@Override
//			public int compare(Integer o1, Integer o2) {
//				if(o1 > o2)
//					return 1;
//				else
//					return -1;
//			}
//		}, true, 1);
		
		//sorting based on key
		JavaPairRDD<Integer, Bedroom> sortedRooms = rooms.sortByKey(true);
		List<Tuple2<Integer, Bedroom>> sRooms = sortedRooms.collect();
		
		for(Tuple2<Integer, Bedroom> t : sRooms ) {
			System.out.println(t._1 + ">>>>>"  + t._2.getCount() + ">>>>>>" + t._2.getAvgPrice());
		}
		
		Map<Integer, Bedroom> roomAveragePrice = sortedRooms.collectAsMap();
		for (java.util.Map.Entry<Integer, Bedroom> r : roomAveragePrice.entrySet()) {
			logger.info(r.getKey() + "---> " + r.getValue().getCount()  + " ----> " + r.getValue().getAvgPrice());
		}

		jsc.close();
	}

}

class Bedroom implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Integer type;
	private Double price;
	private Integer count;
	private Double avgPrice;
	
	public Bedroom() {
		super();
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Double getAvgPrice() {
		return avgPrice;
	}

	public void setAvgPrice(Double avgPrice) {
		this.avgPrice = avgPrice;
	}
}
