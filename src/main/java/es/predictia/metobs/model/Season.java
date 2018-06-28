package es.predictia.metobs.model;

import java.time.Month;

import lombok.AllArgsConstructor;
import lombok.Getter;
@AllArgsConstructor
@Getter
public enum Season {
	WINTER(Month.DECEMBER, Month.JANUARY, Month.FEBRUARY), SPRING(Month.MARCH, Month.APRIL, Month.MAY), SUMMER(Month.JUNE, Month.JULY, Month.AUGUST), AUTUMN(Month.SEPTEMBER, Month.OCTOBER, Month.NOVEMBER);

	
	private final transient Month monthIni;
	private final transient Month monthMid;
	private final transient Month monthEnd;
	
}
