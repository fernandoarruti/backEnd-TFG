package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;

import es.predictia.metobs.model.Observation;

public class MeanStatistic implements IncrementalStatistic {

	private Long count;
	private Double sum;
	
	public MeanStatistic() {
		count = 0l;
		sum = 0d;
	}

	
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue())){
			count += 1l;
			sum += newValue.getValue();
		}
	}

	
	public Collection<Observation> get() {
		Collection<Number> result = new ArrayList<>();
		result.add(sum/count);
		return null; // todo
	}
	
	

	
	
}
