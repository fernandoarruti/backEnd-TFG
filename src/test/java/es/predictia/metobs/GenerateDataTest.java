package es.predictia.metobs;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

/**
 * Test of the generation of the data 
 * @author ferna
 *
 */
public class GenerateDataTest {

	@Test
	public void testGenerateData() throws Exception{
		File outFile = new File("C:\\Users\\ferna\\eclipse-workspace\\springTutorial\\src\\test\\resources\\observations.csv");
		FileWriter writer = new FileWriter(outFile);
		LocalDate start = LocalDate.of(1920,01,01);
		LocalDate end = LocalDate.now();
		// header
		writer.write("date");
		for(int i=0;i<NUMBER_STATIONS;i++) {
			writer.write(";station"+(i+1));
		}
		writer.write("\n");
		while(start.isBefore(end)) {
			writer.write(start.format(DATE_FORMAT));
			for(int i=0;i<NUMBER_STATIONS;i++) {
				Double rand = Math.random();
				writer.write(";");
				if(rand<.95) {
					writer.write(rand.toString());
				}
			}
			writer.write("\n");
			start = start.plusDays(1);
		}
		
		writer.close();
	}

	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");
	private static final Integer NUMBER_STATIONS = 10;
}
