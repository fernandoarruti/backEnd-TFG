package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;

import es.predictia.metobs.model.Observation;

public class SumStatistic implements IncrementalStatistic{
	private Double count;
	
	public SumStatistic() {
		count = 0.0;
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue())){
			count += newValue.getValue();
		
		}
	}

	@Override
	public Collection<Observation> get() {
		Collection<Number> result = new ArrayList<>();
		result.add(count);
		//return result;
		return null;
	}
}
