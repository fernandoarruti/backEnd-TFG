package es.predictia.metobs.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class SeasonYearPair {

	Season season;
	Integer year;
}
