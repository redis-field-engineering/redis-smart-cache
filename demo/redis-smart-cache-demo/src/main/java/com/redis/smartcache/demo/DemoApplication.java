package com.redis.smartcache.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Redis Smart Cache Demo Application.
 *
 */
@SpringBootApplication
public class DemoApplication implements ApplicationRunner {

	@Autowired
	private DataLoader dataLoader;
	@Autowired
	private QueryExecutor queryExecutor;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		dataLoader.execute();
		queryExecutor.execute();
	}

}
