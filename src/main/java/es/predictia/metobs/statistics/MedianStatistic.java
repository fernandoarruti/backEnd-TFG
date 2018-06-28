package es.predictia.metobs.statistics;

import java.util.Collection;

import org.assertj.core.util.Lists;

import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.Variable;

public class MedianStatistic extends PercentileStatistic implements IncrementalStatistic{

	public MedianStatistic(Variable variable){
		super(variable.getBins());
	}
	
	
	public void update(Observation value) {
		super.update(value.getValue());		
	}

	
	public Collection<Observation> get() {
		//return Lists.newArrayList(super.get(50d));
		return null;
	}

}
