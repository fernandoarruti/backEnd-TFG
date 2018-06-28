package es.predictia.metobs.statistics;


import java.util.Collection;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;

import es.predictia.metobs.model.Observation;


public class Top10Statistic  implements IncrementalStatistic {
	private Queue<Observation> top10;
	
	public Top10Statistic() {
		top10 = new PriorityQueue<Observation>(10);
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && (top10.size() < 10 || newValue.getValue() > top10.peek().getValue())){
			if (top10.size() == 10) {
                top10.poll();
			}          
			top10.offer(newValue);
		}
	}

	@Override
	public Collection<Observation> get() {
		return top10;
	}
}
