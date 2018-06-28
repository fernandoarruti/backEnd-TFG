package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class YearMean {
	private String year;
	private Double value;
	private Variable variable;
}
