package com.redis.sidecar.springdemo;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redis.sidecar.SidecarDriver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.lettuce.core.RedisURI;

@Configuration
public class SidecarDataSourceConfiguration {

	@Bean
	public DataSource sidecarDataSource(RedisURI redisURI, DataSourceProperties dataSourceProperties) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:" + redisURI.toString());
		config.setDriverClassName(SidecarDriver.class.getName());
		System.setProperty("sidecar.driver.url", dataSourceProperties.determineUrl());
		System.setProperty("sidecar.driver.class-name", dataSourceProperties.determineDriverClassName());
		System.setProperty("sidecar.metrics-step", "5");
		config.setUsername(dataSourceProperties.determineUsername());
		config.setPassword(dataSourceProperties.determinePassword());
		return new HikariDataSource(config);

	}

}