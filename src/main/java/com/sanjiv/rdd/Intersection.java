package com.sanjiv.rdd;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Intersection {

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(Intersection.class);
		Logger.getLogger("org").setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf().setAppName("NasaLogIntersection").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> julyLogRdd = jsc.textFile("input/nasa_19950701.tsv");
		JavaRDD<String> augLogRdd  = jsc.textFile("input/nasa_19950801.tsv");
		
		JavaRDD<NasaLog> julyLogObjectRdd = julyLogRdd.map(line -> {
			
			String[] lines = line.split("\t");
			NasaLog nasaLog = new NasaLog();
			if(lines.length == 7) {
				nasaLog.setHost(lines[0]);
				nasaLog.setLogname(lines[1]);
				nasaLog.setTime(lines[2]);
				nasaLog.setMethod(lines[3]);
				nasaLog.setUrl(lines[4]);
				nasaLog.setResponse(lines[5]);
				nasaLog.setBytes(lines[6]);
			}
			return nasaLog;
			
		});
		
		logger.info("Total July Log count RDD : " + julyLogObjectRdd.count()); 
		
		JavaRDD<NasaLog> augLogObjectRdd = augLogRdd.map(line -> {
			
			String[] lines = line.split("\t");
			NasaLog nasaLog = new NasaLog();
			if(lines.length == 7) {
				nasaLog.setHost(lines[0]);
				nasaLog.setLogname(lines[1]);
				nasaLog.setTime(lines[2]);
				nasaLog.setMethod(lines[3]);
				nasaLog.setUrl(lines[4]);
				nasaLog.setResponse(lines[5]);
				nasaLog.setBytes(lines[6]);
			}
			return nasaLog;
		});
		
		logger.info("Total Aug Log count RDD : " + augLogObjectRdd.count()); 
	
		JavaRDD<NasaLog> commonLog = julyLogObjectRdd.intersection(augLogObjectRdd);

		JavaRDD<NasaLog> filteredLog = commonLog.filter(line -> excludeHeader(line));
		
		logger.info("Total Log count RDD : " + filteredLog.count()); 
		
		JavaRDD<NasaLog> sampleLog = filteredLog.sample(true, 0.1); 
		
		logger.info("Sample Log count RDD : " + sampleLog.count()); 
		
		sampleLog.saveAsTextFile("output/nasa_log_intersection");
		
		jsc.close();
	}

	private static boolean excludeHeader(NasaLog line) { 
		if(line.getHost()==null && line.getBytes()==null) {
			return false;
		}
		return (!line.getHost().startsWith("host") && !line.getBytes().contains("bytes"));
	}

}


class NasaLog implements Serializable{

	private static final long serialVersionUID = 3141171005133648571L;

	private String host;
	private String logname;
	private String time;
	private String method;
	private String url;
	private String response;
	private String bytes;
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getLogname() {
		return logname;
	}
	public void setLogname(String logname) {
		this.logname = logname;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getResponse() {
		return response;
	}
	public void setResponse(String response) {
		this.response = response;
	}
	public String getBytes() {
		return bytes;
	}
	public void setBytes(String bytes) {
		this.bytes = bytes;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
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
		NasaLog other = (NasaLog) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "NasaLog [host=" + host + ", logname=" + logname + ", time=" + time + ", method=" + method + ", url="
				+ url + ", response=" + response + ", bytes=" + bytes + "]";
	}
	
}








