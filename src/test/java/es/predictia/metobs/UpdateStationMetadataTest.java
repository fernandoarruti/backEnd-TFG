package es.predictia.metobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.Station;
import es.predictia.metobs.model.TemporalFilter;
import es.predictia.metobs.model.Variable;
import es.predictia.metobs.service.ObservationReader;

@RunWith(SpringRunner.class)
@SpringBootTest
public class UpdateStationMetadataTest {

	@Test
	public void testUpdateStationMetadata() throws Exception{

		Field<String> codeField = field("code",String.class);
		Field<String> nameField = field("name",String.class);
		Field<String> providerField = field("provider",String.class);
		Field<Double> latitudeField = field("latitude",Double.class);
		Field<Double> longitudeField = field("longitude",Double.class);
		Field<Double> altitudeField = field("altitude",Double.class);
		Field<LocalDate> dateStartField = field("startDate",LocalDate.class);
		Field<LocalDate> dateEndField = field("endDate",LocalDate.class);
		Field<Double> missingNumberField = field("missingNumber",Double.class);
		
		LOGGER.debug("Deleting content from "+TABLE_NAME);
		dslContext.delete(table(TABLE_NAME));

		Map<String,es.predictia.metobs.model.Station> stationsMeta = new HashMap<>();
		for(Variable variable : Variable.values()){
			LOGGER.debug("Inserting info for variable "+variable.name());
			Field<Boolean> variableField = field(variable.getCode(),Boolean.class);
			List<Station> stations = observationReader.readStationsVariable(variable);//estaciones que contienen datos de la variable
			Integer stationCount = 0;
			for(Station station : stations){
				stationCount++;
				LOGGER.debug("   Reading data for station "+station.getName()+" ("+stationCount+" of "+stations.size()+")");
				List<Observation> data = observationReader.read(variable, station.getName(), Lists.newArrayList(TemporalFilter.NONE)).collect(Collectors.toList());//obtenemos datos de la estacion
				
				Observation startDate = data.stream().filter(o -> {
					if(o.getValue()!=null){
						return !Double.isNaN(o.getValue());
					}
					return false;
				}).findFirst().orElse(null);

				Collections.reverse(data);
				Observation endDate = data.stream().filter(o -> {
					if(o.getValue()!=null){
						return !Double.isNaN(o.getValue());
					}
					return false;
				}).findFirst().orElse(null);
				
				Boolean computeMissing = false;
				
				if(!stationsMeta.containsKey(station.getName())){
					Station newStation = new Station();
					newStation.setCode(station.getCode());
					newStation.setAltitude(station.getAltitude());
					newStation.setName(station.getName());
					stationsMeta.put(newStation.getCode(),newStation);
					computeMissing = true;
				}
				es.predictia.metobs.model.Station newStation = stationsMeta.get(station.getName());
				if(newStation.getLongitude() == null){
					newStation.setLongitude(station.getLongitude());
				}
				if(newStation.getLatitude() == null){
					newStation.setLatitude(station.getLatitude());
				}
				if(newStation.getStartDate() == null){
					newStation.setStartDate(startDate.getDate());
					computeMissing = true;
				}else if(startDate!=null){
					if(newStation.getStartDate().isAfter(startDate.getDate())){
						newStation.setStartDate(startDate.getDate());
						computeMissing = true;
					}
				}
				if(newStation.getEndDate() == null){
					newStation.setEndDate(endDate.getDate());
					computeMissing = true;
				}else if(endDate!=null){
					if(newStation.getEndDate().isBefore(endDate.getDate())){
						newStation.setEndDate(endDate.getDate());
						computeMissing = true;
					}
				}

				if(computeMissing){
					int ini=data.indexOf(startDate);
					int end=data.indexOf(endDate);
					int wrongValues=0;
					int numberValues=0;
					if(startDate!=null){
						for(int i=end;i<=ini;i++){
							if(data.get(i).getValue().isNaN()){
								wrongValues++;	
							}
							numberValues++;						
						}
					}
					newStation.setMissingNumber(100d*wrongValues/(double)numberValues);
				}
				
				Boolean exists = dslContext.fetchCount(dslContext.select(nameField).from(table(TABLE_NAME)).where(codeField.eq(station.getName()))) > 0;

				if(!exists){
					dslContext
						.insertInto(table(TABLE_NAME))
						.columns(codeField, nameField, providerField, latitudeField, longitudeField, altitudeField, dateStartField, dateEndField, variableField, missingNumberField)
						.values(newStation.getCode(), newStation.getName(), PROVIDER, newStation.getLatitude(), newStation.getLongitude(), newStation.getAltitude(), newStation.getStartDate(), newStation.getEndDate(), Boolean.TRUE, newStation.getMissingNumber())
					.execute();	
				}else{
					dslContext.update(table(TABLE_NAME))
						.set(variableField, true)
						.where(codeField.eq(station.getName()))
					.execute();
				}
			}
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateStationMetadataTest.class);

	private static final String PROVIDER = "AEMET";
	private static final String TABLE_NAME = "stations";

	@Autowired private DSLContext dslContext;
	@Autowired private ObservationReader observationReader;

	@Value("${spring.datasource.url}") private String url;
	@Value("${spring.datasource.username}") private String userName;
	@Value("${spring.datasource.password}") private String password;
	@Value("${spring.datasource.driver-class-name}") private String nombre;

}
