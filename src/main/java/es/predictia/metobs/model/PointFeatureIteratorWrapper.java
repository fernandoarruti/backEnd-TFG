package es.predictia.metobs.model;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import ucar.nc2.ft.PointFeature;
import ucar.nc2.ft.PointFeatureIterator;

@AllArgsConstructor
public class PointFeatureIteratorWrapper implements Iterator<PointFeature>{

	private final PointFeatureIterator iterator;

	@Override
	public boolean hasNext() {
		try {
			return iterator.hasNext();
		} catch (IOException e) {
			LOGGER.error("Error checking next value in iterator");
			return false;
		}
	}

	@Override
	public PointFeature next() throws NoSuchElementException{
		
		try {
			return iterator.next();
		} catch (IOException e) {
			LOGGER.error("Error getting next value from iterator");
			throw new NoSuchElementException();
		} 
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PointFeatureIteratorWrapper.class);
	
}
