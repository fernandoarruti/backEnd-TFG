package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;

import es.predictia.metobs.model.Observation;

public class MaxStatistic implements IncrementalStatistic {

	private Observation max;
	
	
	public MaxStatistic() {
		max = new Observation(null,Double.MIN_VALUE);
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && newValue.getValue()>max.getValue()){
			max = newValue;
		}
		
		
	}

	@Override
	public Collection<Observation> get() {
		Collection<Observation> result = new ArrayList<>();
		result.add(max);
		return result;
	}
	
	

	
	
}
