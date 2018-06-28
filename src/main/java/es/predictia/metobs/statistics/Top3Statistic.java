package es.predictia.metobs.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import es.predictia.metobs.model.Observation;

public class Top3Statistic  implements IncrementalStatistic {
	private Queue<Observation> top3;
	
	public Top3Statistic() {
		top3 = new PriorityQueue<Observation>(3);
	}

	@Override
	public void update(Observation newValue) {
		if(!Double.isNaN(newValue.getValue()) && (top3.size() < 3 || newValue.getValue() > top3.peek().getValue())){
			if (top3.size() == 3) {
                top3.poll();
			}          
			top3.offer(newValue);
		}
	}

	@Override
	public Collection<Observation> get() {
		List<Observation> result = new ArrayList<Observation>();
		
		result.addAll(top3);
		Collections.sort(result, Comparator.comparingDouble(Observation ::getValue).reversed());
		
		return result;
	}
}
