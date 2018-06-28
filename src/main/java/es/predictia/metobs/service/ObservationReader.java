package es.predictia.metobs.service;

import java.io.IOException;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


import java.time.Month;
import es.predictia.metobs.model.MonthMean;
import es.predictia.metobs.model.MonthRankValue;
import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.RankObservation;
import es.predictia.metobs.model.Season;
import es.predictia.metobs.model.SeasonRankValue;
import es.predictia.metobs.model.Station;
import es.predictia.metobs.model.StationDB;
import es.predictia.metobs.model.Statistic;
import es.predictia.metobs.model.TemporalFilter;
import es.predictia.metobs.model.Variable;
import es.predictia.metobs.model.YearMean;

public interface ObservationReader{

	public Stream<Observation> read(Variable variable,String station,Collection<TemporalFilter> temporalFilter) throws IOException;
	
	public Map<Station, Double> daily(Variable variable,LocalDate date) throws Exception;
	
	public Map<TemporalFilter,Map<Statistic,Collection<Observation>>> statistic(Variable variable,String station,Collection<Statistic> statistic,Collection<TemporalFilter> temporalFilter
			, List<Observation> data) throws IOException;

	List<Station> readStationsVariable(Variable variable) throws IOException;

	public StationDB getStationData(String code);

	List<MonthMean> readValuesBetween(Variable variable, String stationName,
			LocalDate startDate, LocalDate endDate) throws IOException;

	List<YearMean> readValuesOfYears(List<Month> months, Variable variable, String stationName, LocalDate startDate,  LocalDate endDate) throws IOException;

	List<MonthRankValue> readRankStatisticsOfMonths(List<Month> months, Variable variable, String stationCode, LocalDate startDate,
			LocalDate endDate, Collection<Statistic> stats) throws IOException;

	List<RankObservation> readRankStatisticsOfYear(Integer year, Variable variable, String stationCode,
			Collection<Statistic> stats)
			throws IOException;

	List<RankObservation> readRankStatisticsOfDay(Integer day, Integer month, Variable variable, String stationCode,
			 Collection<Statistic> stats) throws IOException;
	
	List<SeasonRankValue> readRankStatisticsOfSeason(Season s, Variable variable, String stationCode,  Collection<Statistic> stats) throws IOException;

	List<RankObservation> readTotalRankStatistics(Variable variable, String stationCode, Collection<Statistic> stats)
			throws IOException;


	
	
}