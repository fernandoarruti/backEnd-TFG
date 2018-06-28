package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import es.predictia.metobs.model.Observation;

public class Bottom3Statistic implements IncrementalStatistic{
	private Queue<Observation> bot3;
	
	public Bottom3Statistic() {
		bot3= new PriorityQueue<Observation>(3, Collections.reverseOrder());
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && (bot3.size() < 3 || newValue.getValue() < bot3.peek().getValue())){
			if (bot3.size() == 3) {
				bot3.poll().getValue();
			}        
			
			bot3.offer(newValue);
			
		}
	}

	@Override
	public Collection<Observation> get() {
		List<Observation> result = new ArrayList<Observation>();
		
		result.addAll(bot3);
		Collections.sort(result, Comparator.comparingDouble(Observation ::getValue));
		
		return result;
	}
}
