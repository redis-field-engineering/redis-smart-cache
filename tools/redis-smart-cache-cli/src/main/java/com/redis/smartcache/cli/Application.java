package com.redis.smartcache.cli;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggingSystem;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(Application.class);
		app.setLogStartupInfo(false);
//		app.setBannerMode(Banner.Mode.OFF);
		app.run("Interactive");
//		app.run();
	}

}
