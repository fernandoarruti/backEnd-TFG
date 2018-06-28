package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;

import es.predictia.metobs.model.Observation;

public class CountStatistic implements IncrementalStatistic {

	private Long count;
	
	public CountStatistic() {
		count = 0l;
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue())){
			count += 1l;
		}
	}

	@Override
	public Collection<Observation> get() {
		Collection<Number> result = new ArrayList<>();
		result.add(count);
		return null;// TODO
	}

}
