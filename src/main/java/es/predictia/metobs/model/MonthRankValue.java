package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MonthRankValue {
	Integer position;
	String month;
	String year;
	Double value;
	String type;
}
