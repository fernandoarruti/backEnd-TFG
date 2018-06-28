package es.predictia.metobs.statistics;

import java.util.Collection;

import org.assertj.core.util.Lists;

import es.predictia.metobs.model.Observation;

public class StdDesviationStatistic implements IncrementalStatistic {

	private long count;
	private double sum;
	private double sumsq;

	public StdDesviationStatistic() {
		this.count = 0;
		this.sum = 0.0;
		this.sumsq = 0.0;
	}

	@Override
	public void update(Observation x) {
		++this.count;
		if(!x.getValue().isNaN()) {//anadido, ignorariamos valores nan
			++this.count;
			this.sum += x.getValue();
			this.sumsq += x.getValue() * x.getValue();
		}
			
		
	}

	@Override
	public Collection<Observation> get(){
		double deviation = 0.0;

		if (this.count > 1) {
			deviation = Math.sqrt((this.sumsq - this.sum * this.sum / this.count) / (this.count - 1));
		}
		//return Lists.newArrayList(deviation);
		return null;
	}
}
