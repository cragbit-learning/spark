package com.sanjiv.sparksql;

import java.io.Serializable;

public class StrongDatatypeResponse implements Serializable {

	private static final long serialVersionUID = 6082091169006305067L;
	private String country;
	private String occupation;
	private Integer ageMidPoint;
	private Integer salaryMidPoint;
	
	public StrongDatatypeResponse() {
	}

	public StrongDatatypeResponse(String country, String occupation, Integer ageMidPoint, Integer salaryMidPoint) {
		super();
		this.country = country;
		this.occupation = occupation;
		this.ageMidPoint = ageMidPoint;
		this.salaryMidPoint = salaryMidPoint;
	}
	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getOccupation() {
		return occupation;
	}
	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}
	public Integer getAgeMidPoint() {
		return ageMidPoint;
	}
	public void setAgeMidPoint(Integer ageMidPoint) {
		this.ageMidPoint = ageMidPoint;
	}
	public Integer getSalaryMidPoint() {
		return salaryMidPoint;
	}
	public void setSalaryMidPoint(Integer salaryMidPoint) {
		this.salaryMidPoint = salaryMidPoint;
	}
	@Override
	public String toString() {
		return "StrongDatatypeResponse [country=" + country + ", occupation=" + occupation + ", ageMidPoint="
				+ ageMidPoint + ", salaryMidPoint=" + salaryMidPoint + "]";
	}

}
