package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class SeasonRankValue {
	Integer position;
	String season;
	String year;
	Double value;
	String type;
}
