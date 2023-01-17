package com.redis.sidecar.springdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Sidecar Demo Application.
 *
 */
@SpringBootApplication
public class SidecarDemoApplication implements ApplicationRunner {

	@Autowired
	private DataLoader dataLoader;
	@Autowired
	private QueryExecutor queryExecutor;

	public static void main(String[] args) {
		SpringApplication.run(SidecarDemoApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		dataLoader.execute();
		queryExecutor.execute();
	}

}
