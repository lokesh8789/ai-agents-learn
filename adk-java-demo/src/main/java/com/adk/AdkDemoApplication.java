package com.adk;

import com.adk.config.PlivoProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableConfigurationProperties(PlivoProperties.class)
public class AdkDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdkDemoApplication.class, args);
	}

}
