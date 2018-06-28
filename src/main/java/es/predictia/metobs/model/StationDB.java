package es.predictia.metobs.model;

import java.sql.Date;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StationDB {
	private String code;
	private String name;
	private Double longitude;
	private Double latitude;
	private Double altitude;
	private String provider;
	private Date startDate;
	private Date endDate;
	private Double missingNumber;
	private Byte temperatureMax;
	private Byte temperatureMin;
	private Byte temperatureMed;
	private Byte precipitation;
	private Double missings;
}
