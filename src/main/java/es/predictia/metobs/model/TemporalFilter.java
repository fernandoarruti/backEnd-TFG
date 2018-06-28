package es.predictia.metobs.model;

import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public enum TemporalFilter {
	WINTER(Month.DECEMBER ,Month.MARCH), SUMMER(Month.JUNE,Month.SEPTEMBER), SPRING(Month.MARCH,Month.JUNE), AUTUMN(Month.SEPTEMBER,Month.DECEMBER),
	JANUARY(Month.JANUARY),
	FEBRUARY(Month.FEBRUARY),
	MARCH(Month.MARCH),
	APRIL(Month.APRIL),
	MAY(Month.MAY),
	JUNE(Month.JUNE),
	JULY(Month.JULY),
	AUGUST(Month.AUGUST),
	SEPTEMBER(Month.SEPTEMBER),
	OCTOBER(Month.OCTOBER),
	NOVEMBER(Month.NOVEMBER),
	DECEMBER(Month.DECEMBER),	
	NONE(s -> {return true;},TemporalFilterType.OTHER);

	private TemporalFilter (Month month){
		predicate = new Predicate<LocalDate>() {
			public boolean test(LocalDate t) {
				return t.getMonth().getValue()==month.getValue();
			}
		};
		this.temporalFilterType = TemporalFilterType.MONTH;
	}

	private TemporalFilter (Month startMonth, Month endMonth){
		predicate = new Predicate<LocalDate>() {
			public boolean test(LocalDate t) {
					return (t.getMonth().getValue()>=startMonth.getValue()) && (t.getMonth().getValue()<=endMonth.getValue());
			}
		};
		this.temporalFilterType = TemporalFilterType.SEASON;
	}

	private TemporalFilter(Predicate<LocalDate> predicate,TemporalFilterType temporalFilterType){
		this.predicate = predicate;
		this.temporalFilterType = temporalFilterType;
	}

	private final TemporalFilterType temporalFilterType;

	private final Predicate<LocalDate> predicate;

	public Predicate<LocalDate> getPredicate(){
		return predicate;
	}

	public TemporalFilterType getTemporalFilterType() {
		return temporalFilterType;
	}

	public static Collection<TemporalFilter> values(TemporalFilterType temporalFilterType) {
		List<TemporalFilter> filters = new ArrayList<>();
		for(TemporalFilter temporalFilter : TemporalFilter.values()) {
			if(temporalFilter.getTemporalFilterType().equals(temporalFilterType)) {
				filters.add(temporalFilter);
			}
		}
		return filters;
	}


}
