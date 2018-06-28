package es.predictia.metobs.model;


import es.predictia.metobs.statistics.Bottom10Statistic;
import es.predictia.metobs.statistics.Bottom3Statistic;
import es.predictia.metobs.statistics.CountStatistic;
import es.predictia.metobs.statistics.IncrementalStatistic;
import es.predictia.metobs.statistics.MaxStatistic;
import es.predictia.metobs.statistics.MeanStatistic;
import es.predictia.metobs.statistics.MedianStatistic;
import es.predictia.metobs.statistics.MinStatistic;
import es.predictia.metobs.statistics.Prc05Statistic;
import es.predictia.metobs.statistics.Prc10Statistic;
import es.predictia.metobs.statistics.Prc90Statistic;
import es.predictia.metobs.statistics.Prc95Statistic;
import es.predictia.metobs.statistics.StdDesviationStatistic;
import es.predictia.metobs.statistics.SumStatistic;
import es.predictia.metobs.statistics.Top10Statistic;
import es.predictia.metobs.statistics.Top3Statistic;
public enum Statistic {

	MEAN(MeanStatistic.class),
	SUM(SumStatistic.class),
	MAX(MaxStatistic.class),
	MIN(MinStatistic.class),
	MEDIAN(MedianStatistic.class),
	PRC10(Prc10Statistic.class),
	PRC90(Prc90Statistic.class),
	PRC05(Prc05Statistic.class),
	PRC95(Prc95Statistic.class),
	TOP10(Top10Statistic.class),
	BOTTOM10(Bottom10Statistic.class),
	BOTTOM3(Bottom3Statistic.class),
	TOP3(Top3Statistic.class),
	STD(StdDesviationStatistic.class),
	COUNT(CountStatistic.class);
	
	// STD: https://math.stackexchange.com/questions/102978/incremental-computation-of-standard-deviation
	// PRC: http://techblog.molindo.at/2009/11/efficiently-tracking-response-time-percentiles.html

	private Statistic(Class<? extends IncrementalStatistic> computer){
		this.computer = computer;
	}
	
	private final Class<? extends IncrementalStatistic> computer;
	
	public Class<? extends IncrementalStatistic> computer(){
		return computer;
	}
	
	
}
