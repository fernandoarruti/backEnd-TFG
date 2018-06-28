package es.predictia.metobs.statistics;

import java.util.Collection;

import es.predictia.metobs.model.Observation;

public interface IncrementalStatistic {

	public void update(Observation newValue);
	public Collection<Observation> get();
	
}
