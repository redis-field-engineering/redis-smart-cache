package com.redis.sidecar.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.sidecar.Driver;
import com.redis.sidecar.core.AbstractSidecarTests;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

class MetricsTests extends AbstractSidecarTests {

	private static final Logger log = Logger.getLogger(MetricsTests.class.getName());

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);

	private static final List<String> CUSTOMER_IDS = Arrays.asList("ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "BLAUS",
			"BLONP", "BOLID", "BONAP", "BOTTM", "BSBEV", "CACTU", "CENTC", "CHOPS", "COMMI", "CONSH", "DRACD", "DUMON",
			"EASTC", "ERNSH", "FAMIA", "FISSA", "FOLIG", "FOLKO", "FRANK", "FRANR", "FRANS", "FURIB", "GALED", "GODOS",
			"GOURL", "GREAL", "GROSR", "HANAR", "HILAA", "HUNGC", "HUNGO", "ISLAT", "KOENE", "LACOR", "LAMAI", "LAUGB",
			"LAZYK", "LEHMS", "LETSS", "LILAS", "LINOD", "LONEP", "MAGAA", "MAISD", "MEREP", "MORGK", "NORTS", "OCEAN",
			"OLDWO", "OTTIK", "PARIS", "PERIC", "PICCO", "PRINI", "QUEDE", "QUEEN", "QUICK", "RANCH", "RATTC", "REGGC",
			"RICAR", "RICSU", "ROMEY", "SANTG", "SAVEA", "SEVES", "SIMOB", "SPECD", "SPLIR", "SUPRD", "THEBI", "THECR",
			"TOMSP", "TORTU", "TRADH", "TRAIH", "VAFFE", "VICTE", "VINET", "WANDK", "WARTH", "WELLI", "WHITC", "WILMK",
			"WOLZA");

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(POSTGRESQL);
		runScript(backendConnection, "postgres/northwind.sql");
	}

//	@Test
	void testPreparedStatement() throws Exception {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:redis://localhost:6379");
		config.setDriverClassName("com.redis.sidecar.Driver");
		System.setProperty("sidecar.driver.url", POSTGRESQL.getJdbcUrl());
		System.setProperty("sidecar.driver.class-name", POSTGRESQL.getDriverClassName());
		System.setProperty("sidecar.metrics.publish-interval", "1");
		config.setUsername(POSTGRESQL.getUsername());
		config.setPassword(POSTGRESQL.getPassword());
		HikariDataSource ds = new HikariDataSource(config);
		populateDatabase();
		int nThreads = 64;
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		List<Future<Integer>> futures = new ArrayList<>();
		for (int index = 0; index < nThreads; index++) {
			futures.add(executor.submit(new QueryRunnable(ds)));
		}
		for (Future<Integer> future : futures) {
			future.get();
		}
	}

	private static class QueryRunnable implements Callable<Integer> {

		private final HikariDataSource ds;

		public QueryRunnable(HikariDataSource ds) {
			this.ds = ds;
		}

		@Override
		public Integer call() throws Exception {
			int count = Integer.parseInt(System.getProperty(Driver.PROPERTY_PREFIX + ".test-iterations", "100"));
			Random random = new Random();
			for (int index = 0; index < count; index++) {
				try (Connection connection = ds.getConnection()) {
					PreparedStatement statement = connection.prepareStatement(
							"SELECT * FROM orders o INNER JOIN order_details d ON o.order_id = d.order_id"
									+ "                     INNER JOIN products p ON p.product_id = d.product_id"
									+ "                     INNER JOIN customers c ON c.customer_id = o.customer_id"
									+ "                     INNER JOIN employees e ON e.employee_id = o.employee_id"
									+ "                     INNER JOIN employee_territories t ON t.employee_id = e.employee_id"
									+ "                     INNER JOIN categories g ON g.category_id = p.category_id"
									+ "     WHERE d.quantity BETWEEN ? AND ?");
					statement.setInt(1, random.nextInt(10));
					statement.setInt(2, 10 + random.nextInt(10));
					ResultSet resultSet = statement.executeQuery();
					while (resultSet.next()) {
						for (int columnIndex = 1; columnIndex <= resultSet.getMetaData()
								.getColumnCount(); columnIndex++) {
							resultSet.getObject(columnIndex);
						}
					}
				}
			}
			return count;
		}

	}

	private void populateDatabase() throws SQLException {
		Random random = new Random();
		Connection connection = connection(POSTGRESQL);
		int orderCount = 300000;
		String insertOrderSQL = "INSERT INTO orders VALUES (?, ?, ?, '1996-07-04', '1996-08-01', '1996-07-16', 3, 32.3800011, 'Vins et alcools Chevalier', '59 rue de l''Abbaye', 'Reims', NULL, '51100', 'France')";
		String insertOrderDetailsSQL = "INSERT INTO order_details VALUES (?, ?, ?, ?, 0)";
		PreparedStatement insertOrderStatement = connection.prepareStatement(insertOrderSQL);
		PreparedStatement insertOrderDetailsStatement = connection.prepareStatement(insertOrderDetailsSQL);
		for (int index = 0; index < orderCount; index++) {
			int orderId = 20000 + index;
			String customerId = CUSTOMER_IDS.get(random.nextInt(CUSTOMER_IDS.size()));
			int employeeId = 1 + random.nextInt(9);
			int productId = 1 + random.nextInt(77);
			double price = random.nextDouble();
			int quantity = random.nextInt(10000);

			insertOrderStatement.setInt(1, orderId);
			insertOrderStatement.setString(2, customerId);
			insertOrderStatement.setInt(3, employeeId);
			insertOrderStatement.addBatch();

			insertOrderDetailsStatement.setInt(1, orderId);
			insertOrderDetailsStatement.setInt(2, productId);
			insertOrderDetailsStatement.setDouble(3, price);
			insertOrderDetailsStatement.setInt(4, quantity);
			insertOrderDetailsStatement.addBatch();

			if (index % 10000 == 0) {
				insertOrderStatement.executeBatch();
				insertOrderStatement.close();
				insertOrderStatement = connection.prepareStatement(insertOrderSQL);
				insertOrderDetailsStatement.executeBatch();
				insertOrderDetailsStatement.close();
				insertOrderDetailsStatement = connection.prepareStatement(insertOrderDetailsSQL);
			}
		}
		try (Statement countStatement = connection.createStatement()) {
			ResultSet countResultSet = countStatement.executeQuery("SELECT COUNT(*) FROM orders");
			countResultSet.next();
			log.info("#Orders: " + countResultSet.getInt(1));
		}

	}

}
