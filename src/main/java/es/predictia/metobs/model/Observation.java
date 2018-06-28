package es.predictia.metobs.model;

import java.time.LocalDate;

public class Observation implements Comparable<Observation>{
	
	private final LocalDate date;
	private final Double value;
	
	public Observation(LocalDate date, Double value) {
		super();
		this.date = date;
		this.value = value;
	}
	
	public LocalDate getDate() {
		return date;
	}
	
	public Double getValue() {
		return value;
	}

	@Override
	public int compareTo(Observation o) {
		return this.getValue().compareTo(o.getValue());
	}
}
