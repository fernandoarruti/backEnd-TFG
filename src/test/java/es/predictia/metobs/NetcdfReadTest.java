package es.predictia.metobs;

import java.io.File;
import java.time.Instant;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.predictia.metobs.model.PointFeatureIteratorWrapper;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.ft.FeatureCollection;
import ucar.nc2.ft.FeatureDatasetFactoryManager;
import ucar.nc2.ft.FeatureDatasetPoint;
import ucar.nc2.ft.PointFeature;
import ucar.nc2.ft.StationTimeSeriesFeatureCollection;
import ucar.unidata.geoloc.Station;

public class NetcdfReadTest {

	@Test
	public void testRead() throws Exception{
		String variable = "pr";
		Boolean hasData = false;
		
		File outFile = new File("C:\\Users\\ferna\\Desktop\\netcdfFiles\\pr_red_secundaria_aemet.nc");
		
		Formatter errlog = new Formatter(new StringBuilder(), Locale.US);
		FeatureDatasetPoint pointDataset = (FeatureDatasetPoint) FeatureDatasetFactoryManager.open(FeatureType.STATION, outFile.toURI().toURL().toString(), null, errlog);
		List<FeatureCollection> featureCollections = pointDataset.getPointFeatureCollectionList();
		if(featureCollections.size() == 1){
        	if(!(featureCollections.get(0) instanceof StationTimeSeriesFeatureCollection)){
        		throw new IllegalArgumentException("Unknown feature collection type in "+pointDataset.getNetcdfFile().getLocation());
       		}
		}else {
			throw new IllegalArgumentException("Too many features collections in "+pointDataset.getNetcdfFile().getLocation());
		}
  		StationTimeSeriesFeatureCollection featureCollection = (StationTimeSeriesFeatureCollection)featureCollections.get(0);
  		List<Station> stations = featureCollection.getStations();
  		Station station = stations.get(25);
  		
  		PointFeatureIteratorWrapper iterator = new PointFeatureIteratorWrapper(featureCollection.getStationFeature(station).getPointFeatureIterator(STATION_BUFFER_SIZE));

		//Iterable<PointFeature> iterable = () -> iterator;
		//Stream<PointFeature> stream = StreamSupport.stream(iterable.spliterator(),false);
  		
		while(iterator.hasNext()){
			PointFeature feature = iterator.next();
			Instant instant = Instant.ofEpochMilli(feature.getObservationTimeAsCalendarDate().getMillis());
			Double dathum = null;
			try{
				dathum = feature.getData().getScalarDouble(variable);
			}catch(IllegalArgumentException e){
				dathum = (double)feature.getData().getScalarFloat(variable);
			}
			hasData = true;
			LOGGER.debug("Estacion"+station.getDescription()+" "+instant.toString()+": "+dathum);
		}
		Assert.assertTrue(hasData);
	}

	private static final Integer STATION_BUFFER_SIZE = -1; // default buffer=-1
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NetcdfReadTest.class);
}
