package es.predictia.metobs.statistics;

import java.util.Collection;

import org.assertj.core.util.Lists;

import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.Variable;

public class Prc10Statistic extends PercentileStatistic implements IncrementalStatistic{

	public Prc10Statistic(Variable variable){
		super(variable.getBins());
	}
	
	@Override
	public void update(Observation value) {
		super.update(value.getValue());		
	}

	@Override
	public Collection<Observation> get() {
		return null;
		//return Lists.newArrayList(super.get(10d));
	}

}
