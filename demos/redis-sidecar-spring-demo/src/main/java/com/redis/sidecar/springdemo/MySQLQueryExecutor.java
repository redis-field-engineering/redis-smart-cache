package com.redis.sidecar.springdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

@Component
@ConditionalOnExpression(value = "#{ '${database}' matches 'mysql' }")
public class MySQLQueryExecutor implements DisposableBean {

	private static Logger log = LoggerFactory.getLogger(MySQLQueryExecutor.class);

	private static final String QUERY = "SELECT orders.orderNumber, orders.orderDate, orders.requiredDate, orders.shippedDate, orders.status, orders.customerNumber, customers.customerName, orderdetails.productCode, products.productName, orderdetails.quantityOrdered FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber JOIN products ON orderdetails.productCode = products.productCode WHERE orders.orderNumber = ?";

	private final RedisURI redisURI;
	private final DataSourceProperties dataSourceProperties;
	private final Config config;
	private final List<QueryTask> tasks = new ArrayList<>();

	private ProgressBar progressBar;

	public MySQLQueryExecutor(RedisURI redisURI, DataSourceProperties dataSourceProperties, Config config) {
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
		executor.setCorePoolSize(config.getQueryThreads());
		executor.setMaxPoolSize(config.getQueryThreads());
		executor.setThreadNamePrefix("query-task");
		executor.initialize();
		ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
		progressBarBuilder.setTaskName("Querying");
		progressBarBuilder.showSpeed();
		progressBar = progressBarBuilder.build();
		for (int index = 0; index < config.getQueryThreads(); index++) {
			QueryTask task = new QueryTask(dataSource, config.getLoader().getOrders(), progressBar);
			tasks.add(task);
			executor.execute(task);
		}
	}

	private static class QueryTask implements Runnable {

		private final DataSource dataSource;
		private final ProgressBar progressBar;
		private final int totalRows;
		private boolean stopped;

		public QueryTask(DataSource dataSource, int totalRows, ProgressBar progressBar) {
			this.dataSource = dataSource;
			this.totalRows = totalRows;
			this.progressBar = progressBar;
		}

		@Override
		public void run() {
			int index = 0;
			while (!stopped) {
				index++;
				try (Connection connection = dataSource.getConnection();
						PreparedStatement statement = connection.prepareStatement(QUERY)) {
					statement.setInt(1, index % totalRows);
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							for (int columnIndex = 1; columnIndex <= resultSet.getMetaData()
									.getColumnCount(); columnIndex++) {
								resultSet.getObject(columnIndex);
							}
						}
					}
					progressBar.step();
				} catch (SQLException e) {
					log.error("Could not run query", e);
				}
			}
		}

		public void stop() {
			this.stopped = true;
		}

	}

	@Override
	public void destroy() throws Exception {
		tasks.forEach(QueryTask::stop);
		tasks.clear();
		if (progressBar != null) {
			progressBar.close();
		}
	}

}
