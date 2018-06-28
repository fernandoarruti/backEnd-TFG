package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class SeasonYearMean implements Comparable<SeasonYearMean>{
	private Season season;
	private String year;
	private Double value;
	private Variable variable;
	
	@Override
	public int compareTo(SeasonYearMean o) {
		return this.getValue().compareTo(o.getValue());
	}
}
