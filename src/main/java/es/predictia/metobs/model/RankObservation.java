package es.predictia.metobs.model;

import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class RankObservation {

		private String stationCode;
		private String variable;
		private String type;
		private Integer position;
		private Double value;
		private LocalDate date;
}
