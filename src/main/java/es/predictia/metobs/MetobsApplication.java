package es.predictia.metobs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class MetobsApplication {

	public static void main(String[] args) {
		SpringApplication.run(MetobsApplication.class, args);
	}
}
