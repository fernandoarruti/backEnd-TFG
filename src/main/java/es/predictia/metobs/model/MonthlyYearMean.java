package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MonthlyYearMean implements Comparable<MonthlyYearMean>{
	private String month;
	private String year;
	private Double value;
	private Variable variable;
	
	@Override
	public int compareTo(MonthlyYearMean o) {
		return this.getValue().compareTo(o.getValue());
	}
}
