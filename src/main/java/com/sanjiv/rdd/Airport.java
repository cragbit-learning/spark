package com.sanjiv.rdd;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.sanjiv.utils.Util;

/* Create a Spark program to read the airport data from input/airports.text, find all the airports which are located in India
   and output the airport's name, the city's name and country name to output/airports_in_india.text.

   Each row of the input file contains the following columns:
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

   Sample output:
   "Jai prakash narayan", "patna", "India"
   "kempegawda internal airport", "Bangalore", "India"
   ...
 */

public class Airport {

	public static void main(String[] args) {

		Logger logger = Logger.getLogger(Airport.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[2]");

		JavaSparkContext jsc = null;
		JavaRDD<String> airportsRdd = null;
		String separator = ",";

		try {
			jsc = new JavaSparkContext(conf);
		} catch (Exception e) {
			logger.error("Error while estblishing cluster connection" + e);
		}
		
		airportsRdd = jsc.textFile("input/airports.text");
		logger.info("Total RDD : " + airportsRdd.count());
		
		/*
		 * Need to find solution, in case of wrong inclusion....
		 * 
		 * if(airportsRdd.take(1).isEmpty()) {
		 * logger.info("Did not find any records...!"); return; }
		 */
		
		/*
		 * filtering out Indian airport by using filter and value India...
		 * Note giving \" because in input file, country name in double quotes.
		 */
		JavaRDD<String> indianAirportRdd = airportsRdd
				.filter(line -> line.split(Util.COMMA_DELIMITER)[3].equalsIgnoreCase("\"india\""));

		JavaRDD<AirportSort> resultAirportRdd = indianAirportRdd.map(airport -> {
			String[] line = airport.split(Util.COMMA_DELIMITER);
			
			AirportSort airportSort = new AirportSort();
			airportSort.setName( line[1]);
			airportSort.setCity(line[2]);
			airportSort.setCountry( line[3]);
			//return StringUtils.join(new String[] { line[1], line[2], line[3] }, separator);
			return airportSort;
		});
		
		JavaRDD<AirportSort> sortByCity = resultAirportRdd.sortBy(new Function<AirportSort, String>() {
			@Override
			public String call(AirportSort v1) throws Exception {
				return v1.getCity();
			}
		}, false, 2);
		
		logger.info("Total Airport in India : " + sortByCity.count());
		
		sortByCity.saveAsTextFile("output/indian_airport");
		
		/*
		 * for (AirportSort airportSort : sortByCity.collect()) { logger.info("City : "
		 * + airportSort.getCity() + " , name : " + airportSort.getName() +
		 * " , country : " + airportSort.getCountry()); }
		 */
		
		
		
		jsc.close();
	}
}


class AirportSort implements Serializable {
	
	private static final long serialVersionUID = -2685444218382696366L; 
	private String country;
	private String city;
	private String name;
	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AirportSort other = (AirportSort) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return  country + "," + city + "," + name;
	}
	
	
	
}



