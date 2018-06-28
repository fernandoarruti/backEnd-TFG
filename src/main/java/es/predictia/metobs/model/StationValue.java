package es.predictia.metobs.model;

import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StationValue {

	private String code;
	private String name;
	private Double longitude;
	private Double latitude;
	private Double altitude;
	private Double value;
	
	private String provider;
	private LocalDate startDate;
	private LocalDate endDate;
	private Double missingNumber;
	private Boolean temperatureMax;
	private Boolean temperatureMin;
	private Boolean temperatureMed;
	private Boolean precipitation;
	private Integer missings;
	
}
