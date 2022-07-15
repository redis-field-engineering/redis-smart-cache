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

import com.redis.sidecar.AbstractSidecarTests;
import com.redis.sidecar.SidecarDriver;
import com.redis.testcontainers.RedisModulesContainer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

class MetricsTests extends AbstractSidecarTests {

	private static final Logger log = Logger.getLogger(MetricsTests.class.getName());

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);
	@Container
	private static final RedisModulesContainer REDISMOD = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	private static final List<String> CUSTOMER_IDS = Arrays.asList("ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "BLAUS",
			"BLONP", "BOLID", "BONAP", "BOTTM", "BSBEV", "CACTU", "CENTC", "CHOPS", "COMMI", "CONSH", "DRACD", "DUMON",
			"EASTC", "ERNSH", "FAMIA", "FISSA", "FOLIG", "FOLKO", "FRANK", "FRANR", "FRANS", "FURIB", "GALED", "GODOS",
			"GOURL", "GREAL", "GROSR", "HANAR", "HILAA", "HUNGC", "HUNGO", "ISLAT", "KOENE", "LACOR", "LAMAI", "LAUGB",
			"LAZYK", "LEHMS", "LETSS", "LILAS", "LINOD", "LONEP", "MAGAA", "MAISD", "MEREP", "MORGK", "NORTS", "OCEAN",
			"OLDWO", "OTTIK", "PARIS", "PERIC", "PICCO", "PRINI", "QUEDE", "QUEEN", "QUICK", "RANCH", "RATTC", "REGGC",
			"RICAR", "RICSU", "ROMEY", "SANTG", "SAVEA", "SEVES", "SIMOB", "SPECD", "SPLIR", "SUPRD", "THEBI", "THECR",
			"TOMSP", "TORTU", "TRADH", "TRAIH", "VAFFE", "VICTE", "VINET", "WANDK", "WARTH", "WELLI", "WHITC", "WILMK",
			"WOLZA");

	private static final String PROPERTY_PREFIX = "sidecar.test.";

	private static final int ORDER_ID_START = 20000;

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(POSTGRESQL);
		runScript(backendConnection, "postgres/northwind.sql");
	}

	@Test
	void testPreparedStatement() throws Exception {
		HikariConfig config = new HikariConfig();
		int redisPort = intProperty("redis.port", REDISMOD.getFirstMappedPort());
		String redisHost = property("redis.host", REDISMOD.getHost());
		config.setJdbcUrl("jdbc:redis://" + redisHost + ":" + redisPort);
		config.setDriverClassName(SidecarDriver.class.getName());
		config.addDataSourceProperty("sidecar.driver.url", POSTGRESQL.getJdbcUrl());
		config.addDataSourceProperty("sidecar.driver.class-name", POSTGRESQL.getDriverClassName());
		config.setUsername(POSTGRESQL.getUsername());
		config.setPassword(POSTGRESQL.getPassword());
		HikariDataSource ds = new HikariDataSource(config);
		populateDatabase();
		int threads = intProperty("threads", 8);
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		List<Future<Integer>> futures = new ArrayList<>();
		log.info(String.format("Starting %s query threads", threads));
		for (int index = 0; index < threads; index++) {
			futures.add(executor.submit(new QueryRunnable(ds)));
		}
		for (Future<Integer> future : futures) {
			future.get();
		}
	}

	private static class QueryRunnable implements Callable<Integer> {

		private final HikariDataSource ds;
		private final int start = intProperty("start", 10);
		private final int spread = intProperty("spread", 1000);
		private final int iterations = intProperty("iterations", 100);

		public QueryRunnable(HikariDataSource ds) {
			this.ds = ds;
		}

		@Override
		public Integer call() throws Exception {
			Random random = new Random();
			log.info(String.format("Running %,d query iterations", iterations));
			for (int index = 0; index < iterations; index++) {
				try (Connection connection = ds.getConnection()) {
					PreparedStatement statement = connection.prepareStatement(
							"SELECT * FROM orders o INNER JOIN order_details d ON o.order_id = d.order_id"
									+ "                     INNER JOIN products p ON p.product_id = d.product_id"
									+ "                     INNER JOIN customers c ON c.customer_id = o.customer_id"
									+ "                     INNER JOIN employees e ON e.employee_id = o.employee_id"
									+ "                     INNER JOIN employee_territories t ON t.employee_id = e.employee_id"
									+ "                     INNER JOIN categories g ON g.category_id = p.category_id"
									+ "     WHERE o.order_id BETWEEN ? AND ?");
					int startOrder = ORDER_ID_START + random.nextInt(start);
					statement.setInt(1, startOrder);
					statement.setInt(2, startOrder + random.nextInt(spread));
					ResultSet resultSet = statement.executeQuery();
					int rowCount = 0;
					while (resultSet.next()) {
						for (int columnIndex = 1; columnIndex <= resultSet.getMetaData()
								.getColumnCount(); columnIndex++) {
							resultSet.getObject(columnIndex);
						}
						rowCount++;
					}
					if (index % 100 == 0) {
						log.info(String.format("Ran %s queries; rowcount=%,d", index, rowCount));
					}
				}
			}
			return iterations;
		}

	}

	private static int intProperty(String property, int defaultValue) {
		return Integer.parseInt(property(property, String.valueOf(defaultValue)));
	}

	private static String property(String property, String defaultValue) {
		String name = PROPERTY_PREFIX + property;
		String value = System.getenv(name.toUpperCase().replace('.', '_'));
		if (value == null) {
			return System.getProperty(name, defaultValue);
		}
		return value;
	}

	private void populateDatabase() throws SQLException {
		Random random = new Random();
		Connection connection = connection(POSTGRESQL);
		int rowCount = intProperty("rows", 100);
		int maxQty = intProperty("max.quantity", 1000);
		int batchSize = intProperty("batch", 10000);
		String insertOrderSQL = "INSERT INTO orders VALUES (?, ?, ?, '1996-07-04', '1996-08-01', '1996-07-16', 3, 32.3800011, 'Vins et alcools Chevalier', '59 rue de l''Abbaye', 'Reims', NULL, '51100', 'France')";
		String insertOrderDetailsSQL = "INSERT INTO order_details VALUES (?, ?, ?, ?, 0)";
		PreparedStatement insertOrderStatement = connection.prepareStatement(insertOrderSQL);
		PreparedStatement insertOrderDetailsStatement = connection.prepareStatement(insertOrderDetailsSQL);
		log.info(String.format("Populating database with %,d rows", rowCount));
		for (int index = 0; index < rowCount; index++) {
			int orderId = ORDER_ID_START + index;
			String customerId = CUSTOMER_IDS.get(random.nextInt(CUSTOMER_IDS.size()));
			int employeeId = 1 + random.nextInt(9);
			int productId = 1 + random.nextInt(77);
			double price = random.nextDouble();
			int quantity = random.nextInt(maxQty);

			insertOrderStatement.setInt(1, orderId);
			insertOrderStatement.setString(2, customerId);
			insertOrderStatement.setInt(3, employeeId);
			insertOrderStatement.addBatch();

			insertOrderDetailsStatement.setInt(1, orderId);
			insertOrderDetailsStatement.setInt(2, productId);
			insertOrderDetailsStatement.setDouble(3, price);
			insertOrderDetailsStatement.setInt(4, quantity);
			insertOrderDetailsStatement.addBatch();

			if (index % batchSize == 0) {
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
			log.info("#Rows: " + countResultSet.getInt(1));
		}

	}

}
