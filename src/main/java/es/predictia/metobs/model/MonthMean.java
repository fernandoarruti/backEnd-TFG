package es.predictia.metobs.model;



import lombok.AllArgsConstructor;

import lombok.Getter;


@AllArgsConstructor
@Getter
public class MonthMean {
	private String monthName;
	private Double value;
	private String variable;
}
