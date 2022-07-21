package com.redis.sidecar.springdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.redis.sidecar.SidecarDriver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

@Component
@ConditionalOnExpression(value = "#{ '${database}' matches 'postgres' }")
public class PostgresQueryExecutor implements DisposableBean {

	private static Logger log = LoggerFactory.getLogger(PostgresQueryExecutor.class);

	private static final String QUERY = "SELECT * FROM orders o INNER JOIN order_details d ON o.order_id = d.order_id"
			+ "                     INNER JOIN products p ON p.product_id = d.product_id"
			+ "                     INNER JOIN customers c ON c.customer_id = o.customer_id"
			+ "                     INNER JOIN employees e ON e.employee_id = o.employee_id"
			+ "                     INNER JOIN categories g ON g.category_id = p.category_id"
			+ "                     WHERE o.order_id BETWEEN ? AND ?";

	private final Random random = new Random();
	private final RedisURI redisURI;
	private final DataSourceProperties dataSourceProperties;
	private final Config config;
	private boolean stopped;

	public PostgresQueryExecutor(RedisURI redisURI, DataSourceProperties dataSourceProperties, Config config) {
		this.redisURI = redisURI;
		this.dataSourceProperties = dataSourceProperties;
		this.config = config;
	}

	@PostConstruct
	public void executeQueries() {
		if (config.isFlush()) {
			RedisClient.create(redisURI).connect().sync().flushall();
		}
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl("jdbc:" + redisURI.toString());
		hikariConfig.setDriverClassName(SidecarDriver.class.getName());
		System.setProperty("sidecar.driver.url", dataSourceProperties.determineUrl());
		System.setProperty("sidecar.driver.class-name", dataSourceProperties.determineDriverClassName());
		System.setProperty("sidecar.metrics-step", "5");
		hikariConfig.setUsername(dataSourceProperties.determineUsername());
		hikariConfig.setPassword(dataSourceProperties.determinePassword());
		DataSource dataSource = new HikariDataSource(hikariConfig);
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(config.getQuery().getThreads());
		executor.setMaxPoolSize(config.getQuery().getThreads());
		executor.setThreadNamePrefix("query-task");
		executor.initialize();
		log.info("Starting {} threads", config.getQuery().getThreads());
		for (int index = 0; index < config.getQuery().getThreads(); index++) {
			executor.execute(new QueryTask(dataSource, config));
		}
	}

	public void stop() {
		this.stopped = true;
	}

	private class QueryTask implements Runnable {

		private final DataSource dataSource;
		private final Config config;

		public QueryTask(DataSource dataSource, Config config) {
			this.dataSource = dataSource;
			this.config = config;
		}

		@Override
		public void run() {
			int executionCount = 0;
			int totalRowCount = 0;
			while (!isStopped()) {
				try (Connection connection = dataSource.getConnection();
						PreparedStatement statement = connection.prepareStatement(QUERY)) {
					int orderIdStart = random.nextInt(config.getQuery().getCardinality());
					statement.setInt(1, orderIdStart);
					int orderIdEnd = orderIdStart + config.getQuery().getResults() - 1;
					statement.setInt(2, orderIdEnd);
					try (ResultSet resultSet = statement.executeQuery()) {
						int rowCount = 0;
						while (resultSet.next()) {
							for (int columnIndex = 1; columnIndex <= resultSet.getMetaData()
									.getColumnCount(); columnIndex++) {
								resultSet.getObject(columnIndex);
							}
							rowCount++;
						}
						totalRowCount += rowCount;
					}
					executionCount++;
				} catch (SQLException e) {
					log.error("Could not run query", e);
				}
				if (executionCount % 10 == 0) {
					log.debug("Ran {} queries; Avg ResultSet size: {} rows", executionCount,
							totalRowCount / executionCount);
				}
			}
		}

		private boolean isStopped() {
			return stopped;
		}
	}

	@Override
	public void destroy() throws Exception {
		this.stopped = true;
	}

}
