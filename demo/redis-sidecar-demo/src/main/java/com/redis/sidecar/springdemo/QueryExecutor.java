package com.redis.sidecar.springdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class QueryExecutor implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

	private static final String QUERY = "SELECT orders.orderNumber, orders.orderDate, orders.requiredDate, orders.shippedDate, orders.status, orders.customerNumber, customers.customerName, orderdetails.productCode, products.productName, orderdetails.quantityOrdered FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber JOIN products ON orderdetails.productCode = products.productCode, (SELECT SLEEP(?)) as sleep WHERE orders.orderNumber = ?";

	private final RedisURI redisURI;
	private final DataSourceProperties dataSourceProperties;
	private final Config config;
	private final List<QueryTask> tasks = new ArrayList<>();

	private ProgressBar progressBar;

	public QueryExecutor(RedisURI redisURI, DataSourceProperties dataSourceProperties, Config config) {
		this.redisURI = redisURI;
		this.dataSourceProperties = dataSourceProperties;
		this.config = config;
	}

	public void execute() throws InterruptedException, ExecutionException {
		if (config.isFlush()) {
			try (RedisClient client = RedisClient.create(redisURI)) {
				client.connect().sync().flushall();
			}
		}
		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl("jdbc:" + redisURI.toString());
		hikariConfig.setDriverClassName(SidecarDriver.class.getName());
		hikariConfig.addDataSourceProperty("sidecar.driver.url", dataSourceProperties.determineUrl());
		hikariConfig.addDataSourceProperty("sidecar.driver.class-name",
				dataSourceProperties.determineDriverClassName());
		hikariConfig.addDataSourceProperty("sidecar.metrics.step", "5");
		hikariConfig.setUsername(dataSourceProperties.determineUsername());
		hikariConfig.setPassword(dataSourceProperties.determinePassword());
		try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setCorePoolSize(config.getQueryThreads());
			executor.setMaxPoolSize(config.getQueryThreads());
			executor.setThreadNamePrefix("query-task");
			executor.initialize();
			ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
			progressBarBuilder.setTaskName("Querying");
			progressBarBuilder.showSpeed();
			progressBar = progressBarBuilder.build();
			List<Future<Integer>> futures = new ArrayList<>();
			for (int index = 0; index < config.getQueryThreads(); index++) {
				QueryTask task = new QueryTask(dataSource, config.getLoader().getOrders(), progressBar);
				tasks.add(task);
				futures.add(executor.submit(task));
			}
			for (Future<?> future : futures) {
				future.get();
			}
			executor.shutdown();
		}
	}

	private static class QueryTask implements Callable<Integer> {

		private final Random random = new Random();
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
		public Integer call() throws Exception {
			int count = 0;
			while (!stopped) {
				try (Connection connection = dataSource.getConnection();
						PreparedStatement statement = connection.prepareStatement(QUERY)) {
					int orderNumber = random.nextInt(totalRows) + 1;
					statement.setInt(1, random.nextInt(5));
					statement.setInt(2, orderNumber);
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
								resultSet.getObject(index + 1);
							}
						}
					}
					progressBar.step();
					count++;
				} catch (SQLException e) {
					log.error("Could not run query", e);
				}
			}
			return count;
		}

		public void stop() {
			this.stopped = true;
		}

	}

	@Override
	public void close() throws Exception {
		tasks.forEach(QueryTask::stop);
		tasks.clear();
		if (progressBar != null) {
			progressBar.close();
		}
	}

}
