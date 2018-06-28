package es.predictia.metobs.web;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;


import es.predictia.metobs.model.MonthMean;
import es.predictia.metobs.model.MonthRankValue;
import es.predictia.metobs.model.MonthYearPair;
import es.predictia.metobs.model.MonthlyYearMean;
import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.RankObservation;
import es.predictia.metobs.model.Season;
import es.predictia.metobs.model.SeasonRankValue;
import es.predictia.metobs.model.Station;
import es.predictia.metobs.model.StationDB;
import es.predictia.metobs.model.StationValue;
import es.predictia.metobs.model.Statistic;
import es.predictia.metobs.model.Variable;
import es.predictia.metobs.model.YearMean;
import es.predictia.metobs.service.ObservationReader;
import es.predictia.metobs.service.ObservationReaderImpl;



@Controller
public class ObservationController {

	/**
	 * Return values collected of the variable on the in the date given as parameter 
	 * @param variable
	 * @param date
	 * @return list of the values collected in date of variable
	 */
	@RequestMapping(value="/rest/daily")
	public ResponseEntity<List<StationValue>> daily(@RequestParam Variable variable,@RequestParam @DateTimeFormat(iso = ISO.DATE) LocalDate date){
		List<StationValue> data = new ArrayList<>();
		try {
			observationReader.daily(variable, date).entrySet().stream().forEach(d -> {
				data.add(new StationValue(d.getKey().getCode(),d.getKey().getName(),d.getKey().getLongitude(),d.getKey().getLatitude(),d.getKey().getAltitude(),d.getValue(), d.getKey().getProvider(),
						d.getKey().getStartDate(), d.getKey().getEndDate(), d.getKey().getMissingNumber(), d.getKey().getTemperatureMax(), d.getKey().getTemperatureMin(), 
						d.getKey().getTemperatureMed(), d.getKey().getPrecipitation(), d.getKey().getMissings()));
			});
		} catch (IOException e) {
			LOGGER.error("ERROR READING");
			return new ResponseEntity<List<StationValue>>(new ArrayList<>(),HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			LOGGER.error("ERROR READING");
		}
		return new ResponseEntity<List<StationValue>>(data,HttpStatus.OK);
	}

	
	/**
	 * Get metadata of the station with id given as parameter
	 * @param code
	 * @return metadata of the station with code 
	 */
	@RequestMapping(value="/rest/getStationData")
	public ResponseEntity<StationDB> getStationData(@RequestParam String code){
		StationDB data = observationReader.getStationData(code);
		return new ResponseEntity<StationDB>(data,HttpStatus.OK);
	}
	
	/**
	 * Return the monthly mean collected in the station given as parameter between 2 dates
	 * @param stationName
	 * @param startDate
	 * @param endDate
	 * @return months means collected in the station between 2 dates
	 */
	@RequestMapping(value="/rest/getValuesBetween")
	public ResponseEntity<List<MonthMean>> getValuesBetween(@RequestParam String stationName, @RequestParam String startDate, @RequestParam String endDate){

		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		final LocalDate start = LocalDate.parse(startDate, DATE_FORMAT);
		final LocalDate end = LocalDate.parse(endDate, DATE_FORMAT);
		List<MonthMean> monthMeans= new ArrayList<MonthMean>();
		try {
			monthMeans = observationReader.readValuesBetween(Variable.PRECIPITATION,stationName, start, end);

			monthMeans.addAll(observationReader.readValuesBetween(Variable.TEMPERATURE_MAX,stationName, start, end));
			monthMeans.addAll(observationReader.readValuesBetween(Variable.TEMPERATURE_MIN,stationName, start, end));
			monthMeans.addAll(observationReader.readValuesBetween(Variable.TEMPERATURE_MEAN,stationName, start, end));

		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}

		return new ResponseEntity<List<MonthMean>>(monthMeans,HttpStatus.OK);
	}

	/**
	 * Return the year mean of the values collected between 2 dates
	 * @param months
	 * @param stationName
	 * @param startDate
	 * @param endDate
	 * @return the year means collected between the 2 dates given as parameter
	 */
	@RequestMapping(value="/rest/readValuesOfMonths")
	public ResponseEntity<List<YearMean>> readValuesOfMonths(@RequestParam List<Month> months,@RequestParam String stationName, @RequestParam String startDate, @RequestParam String endDate){

		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		final LocalDate start = LocalDate.parse(startDate, DATE_FORMAT);
		final LocalDate end = LocalDate.parse(endDate, DATE_FORMAT);
		
		List<YearMean>yearMeans= new ArrayList<YearMean>();
		if(months.isEmpty()){

			months= Arrays.asList(Month.values());
		}

		try {
			yearMeans = observationReader.readValuesOfYears(months,Variable.TEMPERATURE_MAX,stationName, start, end);	
			
			yearMeans.addAll(observationReader.readValuesOfYears(months,Variable.TEMPERATURE_MIN,stationName, start, end));
			yearMeans.addAll(observationReader.readValuesOfYears(months,Variable.TEMPERATURE_MEAN,stationName, start, end));
			yearMeans.addAll(observationReader.readValuesOfYears(months,Variable.PRECIPITATION,stationName, start, end));
		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}
		return new ResponseEntity<List<YearMean>>(yearMeans,HttpStatus.OK);
	}

	@RequestMapping(value="/rest/readTopStatisticsOfMonth")
	public ResponseEntity<List<MonthRankValue>> readTopStatisticsOfSeason(@RequestParam List<Month> months,@RequestParam String stationCode, @RequestParam String startDate, @RequestParam String endDate, @RequestParam Integer n,  @RequestParam Variable variable)
	{

		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		final LocalDate start = LocalDate.parse(startDate, DATE_FORMAT);
		final LocalDate end = LocalDate.parse(endDate, DATE_FORMAT);
		List<MonthRankValue> rankStats = new ArrayList<MonthRankValue>();
		Collection<Statistic> stats = new ArrayList<Statistic>();
		stats.add(Statistic.TOP3);
		stats.add(Statistic.BOTTOM3);
		if(months.isEmpty()){
			months= Arrays.asList(Month.values());
		}
		try {
			rankStats = observationReader.readRankStatisticsOfMonths( months, variable, stationCode, start, end,  stats);
		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}

		return new ResponseEntity<List<MonthRankValue>>(rankStats, HttpStatus.OK);
	}

	@RequestMapping(value="/rest/readTopStatisticsOfYear")
	public ResponseEntity<List<RankObservation>> readStatitisticsOfYear(@RequestParam Integer year, @RequestParam String stationCode, @RequestParam Integer n, @RequestParam Variable variable)
	{
		Collection<Statistic> stats = new ArrayList<Statistic>();
		stats.add(Statistic.TOP3);
		stats.add(Statistic.BOTTOM3);
		List<RankObservation> rankStats = new ArrayList<RankObservation>();
		try {
			rankStats = observationReader.readRankStatisticsOfYear( year, variable, stationCode, stats);
		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}

		return new ResponseEntity<List<RankObservation>>(rankStats, HttpStatus.OK);
	}

	@RequestMapping(value="/rest/readTopStatisticsOfDay")
	public ResponseEntity<List<RankObservation>> readStatitisticsOfDay(@RequestParam Integer day, @RequestParam Integer month, @RequestParam String stationCode, @RequestParam Integer n, @RequestParam Variable variable)
	{

		List<RankObservation> rankStats = new ArrayList<RankObservation>();		
		Collection<Statistic> stats = new ArrayList<Statistic>();
		stats.add(Statistic.TOP3);
		stats.add(Statistic.BOTTOM3);
		try {
			rankStats = observationReader.readRankStatisticsOfDay( day, month, variable, stationCode, stats);

		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}
		return new ResponseEntity<List<RankObservation>>(rankStats, HttpStatus.OK);
	}

	@RequestMapping(value="/rest/readTotalStatistics")
	public ResponseEntity<List<RankObservation>> readTotalStatitistics(@RequestParam String stationCode, @RequestParam Integer n, @RequestParam Variable variable)
	{
		List<RankObservation> rankStats = new ArrayList<RankObservation>();		
		Collection<Statistic> stats = new ArrayList<Statistic>();
		stats.add(Statistic.TOP3);
		stats.add(Statistic.BOTTOM3);
		try {
			rankStats = observationReader.readTotalRankStatistics( variable, stationCode, stats);

		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}
		return new ResponseEntity<List<RankObservation>>(rankStats, HttpStatus.OK);
	}
	
	
	@RequestMapping(value="/rest/readStatisticsOfSeason")
	public ResponseEntity<List<SeasonRankValue>> readStatisticsOfSeason(@RequestParam String season,@RequestParam String stationCode, @RequestParam Integer n, @RequestParam Variable variable)
	{
		Season s;
		if(season.equals("Winter")){
			s= Season.WINTER;
		}else if(season.equals("Summer")){
			s= Season.SUMMER;
		}else if(season.equals("Spring")){
			s= Season.SPRING;
		}else if(season.equals("Autumn")){
			s= Season.AUTUMN;
		}else{
			return new ResponseEntity<List<SeasonRankValue>>(new ArrayList<>(),HttpStatus.BAD_REQUEST);
		}
		System.out.println(season);
		List<SeasonRankValue> rankStats = new ArrayList<SeasonRankValue>();		
		Collection<Statistic> stats = new ArrayList<Statistic>();
		stats.add(Statistic.TOP3);
		stats.add(Statistic.BOTTOM3);
		try {
			rankStats = observationReader.readRankStatisticsOfSeason( s, variable, stationCode, stats);

		} catch (IOException e) {
			LOGGER.error("ERROR READING");
		}
		return new ResponseEntity<List<SeasonRankValue>>(rankStats, HttpStatus.OK);
	}
	
	
	
	
	
	
	


	@Autowired
	private ObservationReader observationReader;
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationReaderImpl.class);	

}
