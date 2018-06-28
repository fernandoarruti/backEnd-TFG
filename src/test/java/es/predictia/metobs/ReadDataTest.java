package es.predictia.metobs;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import es.predictia.metobs.model.Observation;
import es.predictia.metobs.model.Statistic;
import es.predictia.metobs.model.TemporalFilter;
import es.predictia.metobs.model.TemporalFilterType;
import es.predictia.metobs.model.Variable;
import es.predictia.metobs.service.ObservationReader;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ReadDataTest {

	@Test
	public void testReadData() throws IOException {
		Stream<Observation> stream = observationReader.read(Variable.TEMPERATURE_MAX,"1109",Lists.newArrayList(TemporalFilter.APRIL));
		System.out.println("ENTRA");
		Assert.assertTrue(stream.findFirst().isPresent());
	}

	@Test
	public void testComputeStatistic() throws IOException {

		Collection<Statistic> stats = Lists.newArrayList(Statistic.values());
		StopWatch watch = new StopWatch();
		watch.start();
		Map<TemporalFilter, Map<Statistic, Collection<Observation>>> result = observationReader.statistic(
				Variable.TEMPERATURE_MAX,"1109",stats, TemporalFilter.values(TemporalFilterType.MONTH),null
				);
		for(TemporalFilter temporalFilter : result.keySet()) {//caso valores std modificado en fichero stdstat
			for(Entry<Statistic, Collection<Observation>> entry : result.get(temporalFilter).entrySet()) {
				Assert.assertTrue(!entry.getValue().isEmpty());
				LOGGER.debug(temporalFilter+": "+entry.getKey()+": "+entry.getValue().iterator().next());
			}
		}
		watch.stop();
		LOGGER.debug("Ellapsed time: "+watch.getTotalTimeMillis()+" ms");
	}

	@Autowired
	private ObservationReader observationReader;

	private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataTest.class);

}
