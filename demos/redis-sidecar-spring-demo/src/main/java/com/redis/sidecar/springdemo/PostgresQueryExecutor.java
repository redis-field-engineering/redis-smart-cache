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
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression(value = "#{ '${database}' matches 'postgres' }")
public class PostgresQueryExecutor implements DisposableBean {

	private static Logger log = LoggerFactory.getLogger(PostgresQueryExecutor.class);

	private static final int ORDER_ID_START = 20000;
	private static final String QUERY = "SELECT * FROM orders o INNER JOIN order_details d ON o.order_id = d.order_id"
			+ "                     INNER JOIN products p ON p.product_id = d.product_id"
			+ "                     INNER JOIN customers c ON c.customer_id = o.customer_id"
			+ "                     INNER JOIN employees e ON e.employee_id = o.employee_id"
			+ "                     INNER JOIN employee_territories t ON t.employee_id = e.employee_id"
			+ "                     INNER JOIN categories g ON g.category_id = p.category_id"
			+ "                     WHERE o.order_id BETWEEN ? AND ?";

	private final Random random = new Random();
	private final DataSource sidecarDataSource;
	private final Config config;
	private final TaskExecutor taskExecutor;
	private boolean stopped;

	public PostgresQueryExecutor(DataSource sidecarDataSource, Config config, TaskExecutor taskExecutor) {
		this.sidecarDataSource = sidecarDataSource;
		this.config = config;
		this.taskExecutor = taskExecutor;
	}

	@PostConstruct
	public void executeQueries() {
		taskExecutor.execute(new QueryTask(sidecarDataSource, config));
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
			while (!isStopped()) {
				int rowCount = 0;
				try (Connection connection = dataSource.getConnection();
						PreparedStatement statement = connection.prepareStatement(QUERY)) {
					int startOrder = ORDER_ID_START + random.nextInt(config.getOrderSpread());
					statement.setInt(1, startOrder);
					statement.setInt(2, startOrder + random.nextInt(config.getOrderSpread()));
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							for (int columnIndex = 1; columnIndex <= resultSet.getMetaData()
									.getColumnCount(); columnIndex++) {
								resultSet.getObject(columnIndex);
							}
							rowCount++;
						}
					}
					executionCount++;
				} catch (SQLException e) {
					log.error("Could not run query", e);
				}
				if (executionCount % 100 == 0) {
					log.info("Ran {} queries; #rows: {}", executionCount, rowCount);
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
