package es.predictia.metobs.statistics;

import java.util.Collection;

import org.assertj.core.util.Lists;

import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.Variable;

public class Prc05Statistic extends PercentileStatistic implements IncrementalStatistic{

	public Prc05Statistic(Variable variable){
		super(variable.getBins());
	}
	
	@Override
	public void update(Observation value) {
		//super.update(value);		
	}

	@Override
	public Collection<Observation> get() {
		//return Lists.newArrayList(super.get(05d));
		return null;
	}

}
