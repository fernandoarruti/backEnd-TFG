
package es.predictia.metobs.service;

import static org.jooq.impl.DSL.field;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Month;


import es.predictia.metobs.model.MonthMean;
import es.predictia.metobs.model.MonthRankValue;
import es.predictia.metobs.model.MonthYearPair;
import es.predictia.metobs.model.MonthlyYearMean;
import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.PointFeatureIteratorWrapper;
import es.predictia.metobs.model.RankObservation;
import es.predictia.metobs.model.Season;
import es.predictia.metobs.model.SeasonRankValue;
import es.predictia.metobs.model.SeasonYearMean;
import es.predictia.metobs.model.SeasonYearPair;
import es.predictia.metobs.model.Station;
import es.predictia.metobs.model.StationDB;
import es.predictia.metobs.model.Statistic;
import es.predictia.metobs.model.TemporalFilter;
import es.predictia.metobs.model.TemporalFilterType;
import es.predictia.metobs.model.Variable;
import es.predictia.metobs.model.YearMean;
import es.predictia.metobs.statistics.IncrementalStatistic;
import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.nc2.constants.AxisType;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.ft.FeatureCollection;
import ucar.nc2.ft.FeatureDatasetFactoryManager;
import ucar.nc2.ft.FeatureDatasetPoint;
import ucar.nc2.ft.PointFeature;
import ucar.nc2.ft.StationTimeSeriesFeatureCollection;
import ucar.nc2.units.DateUnit;


@Service
public class ObservationReaderImpl implements ObservationReader{



	@Override
	/**
	 * Returns a Stream with the data of a station given as parameter  of an specific variable 
	 */
	public Stream<Observation> read(Variable variable,String stationName, Collection<TemporalFilter>  temporalFiters) throws IOException {
		File path = new File(basePath,MessageFormat.format(dailyFilePattern,variable.getCode())); 
		Formatter errlog = new Formatter(new StringBuilder(), Locale.US);
		FeatureDatasetPoint pointDataset = (FeatureDatasetPoint) FeatureDatasetFactoryManager.open(FeatureType.STATION, path.toURI().toURL().toString(), null, errlog);

		List<FeatureCollection> featureCollections = pointDataset.getPointFeatureCollectionList();
		if(featureCollections.size() == 1){
			if(!(featureCollections.get(0) instanceof StationTimeSeriesFeatureCollection)){
				throw new IllegalArgumentException(UNKNOWN_FEATURE_COLLECTION+pointDataset.getNetcdfFile().getLocation());
			}
		}else {
			throw new IllegalArgumentException(TOO_MANY_FEATURES_COLLECTIONS+pointDataset.getNetcdfFile().getLocation());
		}
		StationTimeSeriesFeatureCollection featureCollection = (StationTimeSeriesFeatureCollection)featureCollections.get(0);
		ucar.unidata.geoloc.Station station = featureCollection.getStation(stationName);
		if(station==null) {
			return Stream.empty(); 
		}
		PointFeatureIteratorWrapper iterator = new PointFeatureIteratorWrapper(featureCollection.getStationFeature(station).getPointFeatureIterator(STATION_BUFFER_SIZE));
		Iterable<PointFeature> iterable = () -> iterator;
		return StreamSupport.stream(iterable.spliterator(),false).map(feature -> {
			Instant instant = Instant.ofEpochMilli(feature.getObservationTimeAsCalendarDate().getMillis());
			LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
			Double dathum = Double.NaN;
			try{
				dathum = feature.getData().getScalarDouble(variable.getCode());
			}catch(IllegalArgumentException | IOException e){
				try {
					dathum = (double)feature.getData().getScalarFloat(variable.getCode());
				}catch(IOException e2) {
					// do nothing
				}
			}
			return new Observation(localDateTime.toLocalDate(),Math.floor(dathum * 100) / 100);
		});		
	}

	@Override
	public Map<TemporalFilter,Map<Statistic,Collection<Observation>>> statistic(Variable variable,String station,Collection<Statistic> statistics, Collection<TemporalFilter> temporalFilters
			, List<Observation> data) throws IOException{
		Map<TemporalFilter,Map<Statistic,Collection<Observation>>> result = new HashMap<>();


		Map<TemporalFilter,Map<Statistic,IncrementalStatistic>> computers = new HashMap<>();
		for(TemporalFilter temporalFilter : temporalFilters) {
			if(!computers.containsKey(temporalFilter)) {
				computers.put(temporalFilter, new HashMap<Statistic,IncrementalStatistic>());
			}
			for(Statistic statistic : statistics) {
				try {
					@SuppressWarnings("unchecked")
					Constructor<IncrementalStatistic> constructor = (Constructor<IncrementalStatistic>) statistic.computer().getConstructor(Variable.class);
					computers.get(temporalFilter).put(statistic,constructor.newInstance(variable));
				} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					try {
						computers.get(temporalFilter).put(statistic,statistic.computer().newInstance());
					}catch(Exception e1) {
						LOGGER.error("Error getting instance for computing "+statistic.name()+". Ignoring this statistic");
					}
				}
			}
		}
		data.forEach(s -> {
			for(TemporalFilter temporalFilter : temporalFilters) {
				if(!temporalFilter.getPredicate().test(s.getDate())) continue;
				for(Statistic entry : computers.get(temporalFilter).keySet()) {
					computers.get(temporalFilter).get(entry).update(s);
				}
			}
		});
		for(TemporalFilter temporalFilter : temporalFilters) {
			if(!result.containsKey(temporalFilter)) {
				result.put(temporalFilter, new HashMap<Statistic,Collection<Observation>>());
			}
			for(Map.Entry<Statistic,IncrementalStatistic> entry : computers.get(temporalFilter).entrySet()) { 
				result.get(temporalFilter).put(entry.getKey(),entry.getValue().get());
			}
		}
		return result;
	}


	/**
	 * Return the stations which have values of the data given as parameter
	 * 
	 */
	@Override
	@Cacheable("stations")
	public List<Station> readStationsVariable(Variable variable) throws IOException {
		File path = new File(basePath,MessageFormat.format(dailyFilePattern,variable.getCode())); 
		Formatter errlog = new Formatter(new StringBuilder(), Locale.US);
		FeatureDatasetPoint pointDataset = (FeatureDatasetPoint) FeatureDatasetFactoryManager.open(FeatureType.STATION, path.toURI().toURL().toString(), null, errlog);

		List<FeatureCollection> featureCollections = pointDataset.getPointFeatureCollectionList();
		if(featureCollections.size() == 1){
			if(!(featureCollections.get(0) instanceof StationTimeSeriesFeatureCollection)){
				throw new IllegalArgumentException(UNKNOWN_FEATURE_COLLECTION+pointDataset.getNetcdfFile().getLocation());
			}
		}else {
			throw new IllegalArgumentException(TOO_MANY_FEATURES_COLLECTIONS+pointDataset.getNetcdfFile().getLocation());
		}
		StationTimeSeriesFeatureCollection featureCollection = (StationTimeSeriesFeatureCollection)featureCollections.get(0);//netcdf
		
		return featureCollection.getStations().stream().map(toStation).collect(Collectors.toList());
	}

	@Override
	public Map<Station, Double> daily(Variable variable, LocalDate date) throws Exception{
		Map<es.predictia.metobs.model.Station, Double> values= new HashMap<>();

		File path = new File(basePath,MessageFormat.format(dailyFilePattern,variable.getCode())); 

		NetcdfDataset ncFile = NetcdfDataset.openDataset(path.getAbsolutePath());

		ucar.nc2.Variable variableData = ncFile.findVariable(variable.getCode());
		Dimension stationDim = ncFile.findDimension("station");
		CoordinateAxis1D timeAxis = (CoordinateAxis1D)ncFile.findCoordinateAxis(AxisType.Time);

		DateUnit dateUnit = new DateUnit(timeAxis.getUnitsString());
		long currentReferenceTime = dateUnit.getDateOrigin().getTime();
		LocalDate localDateRef = Instant.ofEpochMilli(currentReferenceTime).atZone(ZoneId.of("UTC")).toLocalDate();
		int ix = (int)(date.toEpochDay()-localDateRef.toEpochDay());
		int[] origin = new int[]{0,ix};
		int[] shape = new int[]{stationDim.getLength(),1};
		Array data = variableData.read(origin,shape).reduce(1);

		List<es.predictia.metobs.model.Station> stations = readStationsVariable(variable);

		for(int i=0;i<data.getShape()[0];i++){
			Double value = data.getDouble(i);
			if(!Double.isNaN(value)){
				es.predictia.metobs.model.Station station = stations.get(i);
				values.put(station, value);
			}
		}
		return values;
	}

	private static Function<ucar.unidata.geoloc.Station,Station> toStation = station -> {
		es.predictia.metobs.model.Station newStation = new es.predictia.metobs.model.Station();
		newStation.setCode(station.getName());
		newStation.setName(station.getDescription());
		newStation.setLatitude(station.getLatitude());
		newStation.setLongitude(station.getLongitude());
		newStation.setAltitude(station.getAltitude());
		return newStation;
	};

	@Value("${data.basepath}") private String basePath;
	@Value("${data.daily.files}") private String dailyFilePattern;

	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationReaderImpl.class);	
	private static final Integer STATION_BUFFER_SIZE = -1; // default buffer=-1

	@Override
	public StationDB getStationData(String code) {
		Result<Record> countryRecords = dslContext.select().from(TABLE_NAME).where(codeField.eq(code)).fetch();
		StationDB station=null;

		for (Record record : countryRecords) { // 1 iteration
			station=new StationDB(record.getValue(codeField), record.getValue(nameField), record.getValue(longitudeField), record.getValue(latitudeField),record.getValue(altitudeField), 
					record.getValue(providerField), record.getValue(startDateField), record.getValue(endDateField), record.getValue(missingNumberField), record.getValue(temperatureMaxField),
					record.getValue(temperatureMinField), record.getValue(temperatureMedField), record.getValue(precipitationField), record.getValue(missingsField));
		}
		return station;	
	}


	/**
	 * Returns the observations between 2 dates of a variable collected by a station given as parameter
	 */
	@Override
	public List<MonthMean> readValuesBetween(Variable variable,String stationCode, LocalDate startDate, LocalDate endDate) throws IOException {
		Predicate<Observation> condition = observation -> observation.getDate().isAfter(startDate) && observation.getDate().isBefore(endDate);

		 List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		 long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
		if(data.size() <18 || data == null || daysBetween<20){
			return new ArrayList<>(); 
		}
		Map<Integer,Double> values = new HashMap<>();
		Map<Integer,Integer> counts = new HashMap<>();
		Map<MonthYearPair,Integer> months = new HashMap<>();
		Map<Integer,Integer> numberMonths = new HashMap<>();

		
		
		
		for(Observation obs : data){
			if((data.get(0).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(0).getDate().getYear()==obs.getDate().getYear() && data.get(0).getDate().getDayOfMonth() > 10)
					|| (data.get(data.size()-1).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(data.size()-1).getDate().getYear()==obs.getDate().getYear() && data.get(data.size()-1).getDate().getDayOfMonth() <20)){
				// discard if first or last month have more than 10 days not stored 
				continue;
			}
			
			if(obs.getValue().isNaN()) continue;
			Integer month = obs.getDate().getMonthValue();
			MonthYearPair myp= new MonthYearPair(obs.getDate().getMonthValue(), obs.getDate().getYear());
			if(!counts.containsKey(month)){
				counts.put(month, 0);
				numberMonths.put(month, 0);
			}
			if(!months.containsKey(myp) && Variable.PRECIPITATION.equals(variable)){
				months.put(myp, 0);
				numberMonths.put(myp.getMonth(), numberMonths.get(myp.getMonth())+1);
			}
			
			counts.put(month,counts.get(month)+1);
			if(!values.containsKey(month)){
				values.put(month, 0d);
			}
			values.put(month,values.get(month)+obs.getValue());
		}
		
		if(!Variable.PRECIPITATION.equals(variable)){
			for(Map.Entry<Integer,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/(double)counts.get(entry.getKey()));
			}
		}else{ 
			for(Map.Entry<Integer,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue() / (double)numberMonths.get(entry.getKey()));
			}
		}
		List<MonthMean> monthMeans= new ArrayList<>(12);
		for(Integer month=0;month<12;month++){
			monthMeans.add(new MonthMean(Month.values()[month].name(), values.get(month+1), variable.name()));
		}
		return monthMeans;
	}


	/**
	 * Returns the observations between 2 dates
	 */
	@Override
	public List<YearMean> readValuesOfYears(List<Month> months, Variable variable, String stationCode, LocalDate startDate,  LocalDate endDate) throws IOException {
		Predicate<Observation> condition = observation ->observation.getDate().isAfter(startDate) && observation.getDate().isBefore(endDate) && months.contains(observation.getDate().getMonth());
		
		List<Observation> data= readObservationsByPredicate(variable, stationCode, condition);
		Map<Integer,Double> values = new HashMap<>();
		Map<Integer,Integer> counts = new HashMap<>();
		
		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
		if(daysBetween<20 || data.isEmpty() || data.size() < 20){
			return new ArrayList<YearMean>(); 
		}
		
		for(Observation obs : data){ //go over value list
			if(obs.getValue().isNaN()) continue;
			if((data.get(0).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(0).getDate().getYear()==obs.getDate().getYear() && data.get(0).getDate().getDayOfMonth() > 10)
					|| (data.get(data.size()-1).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(data.size()-1).getDate().getYear()==obs.getDate().getYear() && data.get(data.size()-1).getDate().getDayOfMonth() <20)){
				// discard if first or last month have more than 10 days not stored
				continue;
			}
			
			if(!counts.containsKey(obs.getDate().getYear())){
				counts.put(obs.getDate().getYear(), 0);
			}
			
			
			counts.put(obs.getDate().getYear(),counts.get(obs.getDate().getYear())+1);

			if(!values.containsKey(obs.getDate().getYear())){
				values.put(obs.getDate().getYear(), 0d);
			}
			
			values.put(obs.getDate().getYear(),values.get(obs.getDate().getYear())+obs.getValue());
		}
		if(!Variable.PRECIPITATION.equals(variable)){
			for(Map.Entry<Integer,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/(double)counts.get(entry.getKey()));
			}
		}else{
			for(Map.Entry<Integer,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue());
			}
		}
		List<YearMean> yearMeans= new ArrayList<>();

		for(Map.Entry<Integer,Double> entry : values.entrySet()){ 
			if((months.size() == 12 && counts.get(entry.getKey()) <320) || (months.size() < 12 && counts.get(entry.getKey()) < 20 * months.size())){
				continue;
			}
			yearMeans.add(new YearMean(entry.getKey().toString(), entry.getValue(), variable));
		}
		Collections.sort(yearMeans, Comparator.comparing(YearMean ::getYear));
		return yearMeans;
	}





	@Override
	public List<MonthRankValue> readRankStatisticsOfMonths(List<Month> months, Variable variable, String stationCode, LocalDate startDate,  LocalDate endDate, 
			Collection<Statistic> stats) throws IOException {		
		Predicate<Observation> condition = observation -> observation.getDate().isAfter(startDate) && observation.getDate().isBefore(endDate) && months.contains(observation.getDate().getMonth());
		List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		
		Map<MonthYearPair,Double> values = new HashMap<>();
		Map<MonthYearPair,Integer> counts = new HashMap<>();
		
		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
		if(daysBetween<20){
			return new ArrayList<>(); 
		}
		if(data.isEmpty()) {
			return new ArrayList<>(); 
		}
		

		for(Observation obs : data){ //go over value list
			if(obs.getValue().isNaN()) continue;
			if((data.get(0).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(0).getDate().getYear()==obs.getDate().getYear() && data.get(0).getDate().getDayOfMonth() > 10)
					|| (data.get(data.size()-1).getDate().getMonth().equals(obs.getDate().getMonth()) && data.get(data.size()-1).getDate().getYear()==obs.getDate().getYear() && data.get(data.size()-1).getDate().getDayOfMonth() <20)){
				// discard if first or last month have more than 10 days not stored
				continue;
			}
			

			MonthYearPair my= new MonthYearPair(obs.getDate().getMonthValue(), obs.getDate().getYear());
			if(!counts.containsKey(my)){
				counts.put(my, 0);
			}
			counts.put(my,counts.get(my)+1);

			if(!values.containsKey(my)){
				values.put(my, 0d);
			}
			values.put(my,values.get(my)+obs.getValue());
		}
		if(!Variable.PRECIPITATION.equals(variable)){
			for(Map.Entry<MonthYearPair,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/(double)counts.get(entry.getKey()));
			}
		}else{
			for(Map.Entry<MonthYearPair,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/10d);
			}
		}


		List<MonthlyYearMean> monthMeans= new ArrayList<>();

		for(Map.Entry<MonthYearPair,Double> entry : values.entrySet()){ // par valor variable
			monthMeans.add(new MonthlyYearMean(Month.values()[entry.getKey().getMonth()-1].name(),entry.getKey().getYear().toString(),entry.getValue(), variable));
		}
		
		List<MonthRankValue> result = new ArrayList<>();
		
		for(Statistic stat : stats){	
				result.addAll(getRankStatistic(monthMeans, stat));
		}

		return result;


	}

	private List<MonthRankValue> getRankStatistic(List<MonthlyYearMean> lista , Statistic stat) {
		Queue<MonthlyYearMean> rankVals;
		if(stat.equals(Statistic.TOP3)){
			rankVals = new PriorityQueue<>(3);
		}else{
			rankVals = new PriorityQueue<>(3, Collections.reverseOrder());
		}
		List<MonthlyYearMean> result = new ArrayList<>();
		for(MonthlyYearMean my: lista){
			if(stat.equals(Statistic.TOP3)){

				if(!Double.isNaN(my.getValue()) && (rankVals.size() < 3 || my.getValue() > rankVals.peek().getValue())){
					if (rankVals.size() == 3) {						
						rankVals.poll();
					}          
					rankVals.offer(my);
				}
			}else{
				if(!Double.isNaN(my.getValue()) && (rankVals.size() < 3 || my.getValue() < rankVals.peek().getValue())){
					if (rankVals.size() == 3) {
						rankVals.poll();
					}          
					rankVals.offer(my);
				}
			}
		}
		result.addAll(rankVals);
		if(stat.equals(Statistic.TOP3)){
			Collections.sort(result, Comparator.comparingDouble(MonthlyYearMean ::getValue).reversed());
		}else{
			Collections.sort(result, Comparator.comparingDouble(MonthlyYearMean ::getValue));
		}
		List<MonthRankValue> rankList = new ArrayList<>();
		int i=1;
		for(MonthlyYearMean mym: result) {
			rankList.add(new MonthRankValue(i, mym.getMonth(), mym.getYear(), Math.floor(mym.getValue() * 100) / 100, stat.name()));
			i++;
		}

		return rankList;
	}

	@Override
	public List<RankObservation> readRankStatisticsOfYear(Integer year, Variable variable, String stationCode, Collection<Statistic> stats) throws IOException {
		Predicate<Observation> condition = observation -> observation.getDate().getYear() == year && !observation.getValue().isNaN();
		
		 List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		if(data.isEmpty()) {
			return new ArrayList<>(); 
		}
		List<RankObservation> resultados = new ArrayList<>();
		Map<TemporalFilter, Map<Statistic, Collection<Observation>>> returns = statistic(variable, stationCode, stats,  TemporalFilter.values(TemporalFilterType.OTHER), data);
		for(TemporalFilter temporalFilter : returns.keySet()) {//caso valores std modificado en fichero stdstat
			for(Entry<Statistic, Collection<Observation>> entry : returns.get(temporalFilter).entrySet()) {
				int i=1;
				for(Observation s: entry.getValue()){
					resultados.add(new RankObservation(stationCode, variable.name(), entry.getKey().name(),i, s.getValue(), s.getDate()));
					i++;
				}
			}
		}
		return resultados;
	}


	@Override
	public List<RankObservation> readRankStatisticsOfDay(Integer day, Integer month, Variable variable, String stationCode, Collection<Statistic> stats) throws IOException {
		
		Predicate<Observation> condition = observation -> observation.getDate().getDayOfMonth() == day && observation.getDate().getMonthValue() == month && !observation.getValue().isNaN();
		
		List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		
		if(data.isEmpty()) {
			return new ArrayList<>(); 
		}

		List<RankObservation> results = new ArrayList<>();
		Map<TemporalFilter, Map<Statistic, Collection<Observation>>> returns = statistic(variable, stationCode, stats,  TemporalFilter.values(TemporalFilterType.OTHER), data);
		for(TemporalFilter temporalFilter : returns.keySet()) {//caso valores std modificado en fichero stdstat
			for(Entry<Statistic, Collection<Observation>> entry : returns.get(temporalFilter).entrySet()) {
				int i=1;
				for(Observation s: entry.getValue()){
					results.add(new RankObservation(stationCode, variable.name(), entry.getKey().name(),i, s.getValue(), s.getDate()));
					i++;
				}
			}
		}
		return results;
	}

	@Override
	public List<RankObservation> readTotalRankStatistics(Variable variable, String stationCode, Collection<Statistic> stats) throws IOException {
		Predicate<Observation> condition = observation -> !observation.getValue().isNaN();
		List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		if(data.isEmpty()) {
			return new ArrayList<>(); 
		}
		List<RankObservation> results = new ArrayList<>();
		Map<TemporalFilter, Map<Statistic, Collection<Observation>>> returns = statistic(variable, stationCode, stats,  TemporalFilter.values(TemporalFilterType.OTHER), data);
		for(TemporalFilter temporalFilter : returns.keySet()) {//caso valores std modificado en fichero stdstat
			for(Entry<Statistic, Collection<Observation>> entry : returns.get(temporalFilter).entrySet()) {

				int i=1;
				for(Observation s: entry.getValue()){
					results.add(new RankObservation(stationCode, variable.name(), entry.getKey().name(),i, s.getValue(), s.getDate()));
					i++;
				}
			}
		}
		return results;

	}


	@Override
	public List<SeasonRankValue> readRankStatisticsOfSeason(Season s, Variable variable, String stationCode, 
			Collection<Statistic> stats) throws IOException {
		
		Predicate<Observation> condition= observation -> (observation.getDate().getMonth().equals(s.getMonthIni()) || observation.getDate().getMonth().equals(s.getMonthEnd()) || observation.getDate().getMonth().equals(s.getMonthMid())) && !observation.getValue().isNaN();
		List<Observation> data = readObservationsByPredicate(variable, stationCode, condition);
		if(data.isEmpty()){
			return new ArrayList<SeasonRankValue>(); 
		}	
		Map<SeasonYearPair, Double> values = new HashMap<>();
		Map<SeasonYearPair,Integer> counts = new HashMap<>();
		for(Observation obs: data){		
			SeasonYearPair sy;
			if(obs.getDate().getMonth()!= Month.DECEMBER){
				sy = new SeasonYearPair(s, obs.getDate().getYear());
			}else{
				sy = new SeasonYearPair(s, obs.getDate().getYear()+1);
			}

			if(!counts.containsKey(sy)){
				counts.put(sy, 0);
			}
			counts.put(sy,counts.get(sy)+1);

			if(!values.containsKey(sy)){
				values.put(sy, 0d);
			}
			values.put(sy,values.get(sy)+obs.getValue());	
		}

		if(!Variable.PRECIPITATION.equals(variable)){
			for(Map.Entry<SeasonYearPair,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/(double)counts.get(entry.getKey()));
			}
		}else{
			for(Map.Entry<SeasonYearPair,Double> entry : values.entrySet()){
				entry.setValue(entry.getValue()/10d);
			}
		}

		List<SeasonYearMean> seasonMeans= new ArrayList<>();

		for(Map.Entry<SeasonYearPair,Double> entry : values.entrySet()){ // par valor variable
			seasonMeans.add(new SeasonYearMean(entry.getKey().getSeason() , entry.getKey().getYear().toString(), entry.getValue(), variable));
		}
		List<SeasonRankValue> result= new ArrayList<>();
		for(Statistic stat : stats){
				result.addAll(getRankStatisticOfSeason(seasonMeans, stat));
		}
	
		return result;
	}
	
	private List<Observation> readObservationsByPredicate(Variable variable, String stationCode, Predicate<Observation> condition) throws IOException {
		File path = new File(basePath,MessageFormat.format(dailyFilePattern,variable.getCode())); 
		Formatter errlog = new Formatter(new StringBuilder(), Locale.US);
		FeatureDatasetPoint pointDataset = (FeatureDatasetPoint) FeatureDatasetFactoryManager.open(FeatureType.STATION, path.toURI().toURL().toString(), null, errlog);

		List<FeatureCollection> featureCollections = pointDataset.getPointFeatureCollectionList();
		if(featureCollections.size() == 1){
			if(!(featureCollections.get(0) instanceof StationTimeSeriesFeatureCollection)){
				throw new IllegalArgumentException(UNKNOWN_FEATURE_COLLECTION+pointDataset.getNetcdfFile().getLocation());
			}
		}else {
			throw new IllegalArgumentException(TOO_MANY_FEATURES_COLLECTIONS+pointDataset.getNetcdfFile().getLocation());
		}
		StationTimeSeriesFeatureCollection featureCollection = (StationTimeSeriesFeatureCollection)featureCollections.get(0);
		ucar.unidata.geoloc.Station station = featureCollection.getStation(stationCode);
		if(station==null) {
			return new ArrayList<>(); 
		}
		PointFeatureIteratorWrapper iterator = new PointFeatureIteratorWrapper(featureCollection.getStationFeature(station).getPointFeatureIterator(STATION_BUFFER_SIZE));
		Iterable<PointFeature> iterable = () -> iterator;		
		 List<Observation> data = StreamSupport.stream(iterable.spliterator(),false).map(feature -> {
			Instant instant = Instant.ofEpochMilli(feature.getObservationTimeAsCalendarDate().getMillis());
			LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

			Double dathum = Double.NaN;
			try{
				dathum = feature.getData().getScalarDouble(variable.getCode());
			}catch(IllegalArgumentException | IOException e){
				try {
					dathum = (double)feature.getData().getScalarFloat(variable.getCode());
				}catch(IOException e2) {
					// do nothing
				}
			}
			return new Observation(localDateTime.toLocalDate(),Math.floor(dathum * 100) / 100);
		}).filter(condition).collect(Collectors.toList()); // aqui tenemos lista con los datos necesarios, observaciones de mes
		return data;
	}

	private List<SeasonRankValue> getRankStatisticOfSeason(List<SeasonYearMean> lista , Statistic stat) {
		Queue<SeasonYearMean> rankVals;
		if(stat.equals(Statistic.TOP3)){
			rankVals = new PriorityQueue<>(3);
		}else{
			rankVals = new PriorityQueue<>(3, Collections.reverseOrder());
		}
		List<SeasonYearMean> result = new ArrayList<>();
		for(SeasonYearMean sy: lista){
			if(stat.equals(Statistic.TOP3)){

				if(!Double.isNaN(sy.getValue()) && (rankVals.size() < 3 || sy.getValue() > rankVals.peek().getValue())){
					if (rankVals.size() == 3) {						
						rankVals.poll();
					}          
					rankVals.offer(sy);
				}
			}else{
				if(!Double.isNaN(sy.getValue()) && (rankVals.size() < 3 || sy.getValue() < rankVals.peek().getValue())){
					if (rankVals.size() == 3) {
						rankVals.poll();
					}          
					rankVals.offer(sy);
				}
			}
		}
		result.addAll(rankVals);
		if(stat.equals(Statistic.TOP3)){
			Collections.sort(result, Comparator.comparingDouble(SeasonYearMean ::getValue).reversed());
		}else{
			Collections.sort(result, Comparator.comparingDouble(SeasonYearMean ::getValue));
		}
		List<SeasonRankValue> rankList = new ArrayList<SeasonRankValue>();
		int i=1;
		for(SeasonYearMean mym: result) {
			rankList.add(new SeasonRankValue(i, mym.getSeason().name(), mym.getYear(), Math.floor(mym.getValue() * 100) / 100, stat.name()));
			i++;
		}

		return rankList;
	}

	@Autowired private DSLContext dslContext;
	private static final Field<String> codeField = field("code",String.class);
	private static final Field<String> nameField = field("name",String.class);
	private static final Field<Double> longitudeField = field("longitude",Double.class);
	private static final Field<Double> latitudeField = field("latitude",Double.class);
	private static final Field<Double> altitudeField = field("altitude",Double.class);
	private static final Field<String> providerField = field("provider",String.class);
	private static final Field<Date> startDateField = field("startDate",Date.class);
	private static final  Field<Date> endDateField = field("endDate",Date.class);
	private static final Field<Double> missingNumberField = field("missingNumber",Double.class);
	private static final Field<Byte> temperatureMaxField = field("tasmax",Byte.class);
	private static final Field<Byte> temperatureMinField = field("tasmin",Byte.class);
	private static final Field<Byte> temperatureMedField = field("tasmed",Byte.class);
	private static final Field<Byte> precipitationField = field("pr",Byte.class);
	private static final Field<Double> missingsField = field("missingNumber",Double.class);
	private static final String UNKNOWN_FEATURE_COLLECTION = "Unknown feature collection type in ";
	private static final String TOO_MANY_FEATURES_COLLECTIONS = "Too many features collections in ";


	@Value("${spring.datasource.url}") private String url;
	@Value("${spring.datasource.username}") private String userName;
	@Value("${spring.datasource.password}") private String password;
	@Value("${spring.datasource.driver-class-name}") private String nombre;
	private static final String TABLE_NAME = "stations";
}
