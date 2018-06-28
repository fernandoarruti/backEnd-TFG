package es.predictia.metobs.model;

import java.io.Serializable;
import java.time.LocalDate;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
@EqualsAndHashCode
public class Station implements Serializable{

	private static final long serialVersionUID = -2971075350647143601L;
	
	private String code;
	private String name;
	private Double longitude;
	private Double latitude;
	private Double altitude;
	private String provider;
	private transient LocalDate startDate;
	private transient LocalDate endDate;
	private Double missingNumber;
	private Boolean temperatureMax;
	private Boolean temperatureMin;
	private Boolean temperatureMed;
	private Boolean precipitation;
	private int missings;
	private int numberValues;
}
