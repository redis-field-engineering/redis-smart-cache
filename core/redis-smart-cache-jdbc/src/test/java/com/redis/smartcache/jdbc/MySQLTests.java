package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Fields;

@SuppressWarnings("unchecked")
class MySQLTests extends AbstractIntegrationTests {

	@Container
	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>(MySQLContainer.NAME);

	@Override
	protected JdbcDatabaseContainer<?> getBackend() {
		return MYSQL;
	}

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		Connection backendConnection = backendConnection(MYSQL);
		runScript(backendConnection, "mysql/northwind.sql");
	}

	@Test
	void testSimpleStatement() throws Exception {
		Consumer<Config> configurer = c -> {
			c.getMetrics().setEnabled(true);
			c.getMetrics().setStep(io.airlift.units.Duration.valueOf("100ms"));
		};
		testSimpleStatement("SELECT * FROM Product", MYSQL, configurer);
		testSimpleStatement("SELECT * FROM Category", MYSQL, configurer);
		testSimpleStatement("SELECT * FROM Supplier", MYSQL, configurer);
		testSimpleStatement("SELECT * FROM SalesOrder", MYSQL, configurer);
		testSimpleStatement("SELECT * FROM OrderDetail", MYSQL, configurer);
		RedisModulesCommands<String, String> commands = redisConnection.sync();
		Awaitility.await().timeout(Duration.ofSeconds(1)).until(() -> commands.keys("smartcache:query:*").size() == 5);
		String index = "smartcache-query-idx";
		awaitUntil(() -> !RedisModulesUtils.indexInfo(() -> commands.ftInfo(index)).isEmpty());
		SearchResults<String, String> results = commands.ftSearch(index, "*");
		Assertions.assertEquals(5, results.size());
		for (Document<String, String> doc : results) {
			Assertions
					.assertTrue(doc.get(Fields.TAG_SQL).equalsIgnoreCase("select * from " + doc.get(Fields.TAG_TABLE)));
		}
		Assertions.assertEquals("459d4345", redisConnection.sync().hget("smartcache:query:459d4345", "id"));
	}

	@Test
	void testUpdateAndGetResultSet() throws Exception {
		testUpdateAndGetResultSet(MYSQL, "SELECT * FROM Employee");
	}

	@Test
	void testPreparedStatement() throws Exception {
		testPreparedStatement(MYSQL, "SELECT * FROM Employee WHERE mgrid = ?", 5);
	}

	@Test
	void testCallableStatementGetResultSet() throws Exception {
		testCallableStatementGetResultSet(MYSQL, "SELECT * FROM Employee WHERE mgrid = 5");
	}

	@Test
	void testResultSetMetadata() throws Exception {
		testResultSetMetaData("SELECT * FROM Employee", MYSQL);
	}

	@Test
	void testConnect() throws ClassNotFoundException, SQLException {
		Class.forName("com.redis.smartcache.Driver");
		Properties p = new Properties();
		p.setProperty("smartcache.driver.class-name", MYSQL.getDriverClassName());
		p.setProperty("smartcache.driver.url", MYSQL.getJdbcUrl());
		p.setProperty("user", MYSQL.getUsername());
		p.setProperty("password", MYSQL.getPassword());
		try (Connection connection = DriverManager.getConnection("jdbc:" + redis.getRedisURI(), p)) {
			Statement stmt = connection.createStatement();
			boolean result = stmt.execute("select * from Employee");
			Assertions.assertTrue(result);
			ResultSet resultSet = stmt.getResultSet();
			while (resultSet.next()) {
				Assertions.assertNotNull(resultSet.getString(1));
				Assertions.assertNotNull(resultSet.getString(2));
				Assertions.assertNotNull(resultSet.getString(3));
			}
		}
	}
}
