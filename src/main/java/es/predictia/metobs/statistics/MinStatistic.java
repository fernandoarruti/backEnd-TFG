package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;

import es.predictia.metobs.model.Observation;

public class MinStatistic implements IncrementalStatistic {
	private Observation min;
	
	
	public MinStatistic() {
		min = new Observation(null, Double.MAX_VALUE);
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && newValue.getValue()<min.getValue()){
			min = newValue;
		}
	}

	@Override
	public Collection<Observation> get() {
		Collection<Observation> result = new ArrayList<>();
		result.add(min);
		return result;
	}	
	
}
