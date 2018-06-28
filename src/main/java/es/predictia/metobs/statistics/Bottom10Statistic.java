package es.predictia.metobs.statistics;

import java.util.Collection;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;

import es.predictia.metobs.model.Observation;

public class Bottom10Statistic implements IncrementalStatistic{
	private Queue<Observation> bot10;
	
	public Bottom10Statistic() {
		bot10= new PriorityQueue<Observation>(10, Collections.reverseOrder());
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && (bot10.size() < 10 || newValue.getValue() < bot10.peek().getValue())){
			if (bot10.size() == 10) {
				bot10.poll();;
			}        
		
			bot10.offer(newValue);
			
		}
	}

	@Override
	public Collection<Observation> get() {
		return bot10;
	}
}
