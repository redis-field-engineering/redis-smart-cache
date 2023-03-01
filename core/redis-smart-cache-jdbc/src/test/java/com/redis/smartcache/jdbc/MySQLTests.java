package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
import com.redis.smartcache.core.QueryWriter;

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
		testSimpleStatement(MYSQL, "SELECT * FROM Product");
		testSimpleStatement(MYSQL, "SELECT * FROM Category");
		testSimpleStatement(MYSQL, "SELECT * FROM Supplier");
		testSimpleStatement(MYSQL, "SELECT * FROM SalesOrder");
		testSimpleStatement(MYSQL, "SELECT * FROM OrderDetail");
		RedisModulesCommands<String, String> commands = redisConnection.sync();
		Awaitility.await().until(() -> commands.keys("smartcache:queries:*").size() == 5);
		String index = "smartcache-queries-idx";
		Awaitility.await().until(() -> !RedisModulesUtils.indexInfo(() -> commands.ftInfo(index)).isEmpty());
		SearchResults<String, String> results = commands.ftSearch(index, "*");
		Assertions.assertEquals(5, results.size());
		for (Document<String, String> doc : results) {
			Assertions.assertTrue(doc.get(QueryWriter.FIELD_SQL)
					.equalsIgnoreCase("select * from " + doc.get(QueryWriter.FIELD_TABLE)));
		}
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
		testResultSetMetaData(MYSQL, "SELECT * FROM Employee");
	}

	@Test
	void testConnect() throws ClassNotFoundException, SQLException {
		Class.forName("com.redis.smartcache.Driver");
		Properties p = new Properties();
		p.setProperty("smartcache.driver.class-name", MYSQL.getDriverClassName());
		p.setProperty("smartcache.driver.url", MYSQL.getJdbcUrl());
		p.setProperty("user", MYSQL.getUsername());
		p.setProperty("password", MYSQL.getPassword());
		// step2 create the connection object
		Connection connection = DriverManager.getConnection("jdbc:" + redis.getRedisURI(), p);

		// step3 create the statement object
		Statement stmt = connection.createStatement();

		// step4 execute query
		ResultSet rs = stmt.executeQuery("select * from Employee");
		while (rs.next()) {
			System.out.println(rs.getString(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
		}
		connection.close();
	}
}
