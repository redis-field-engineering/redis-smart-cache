package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.smartcache.Driver;
import com.redis.smartcache.core.Config;

class PostgresTests extends AbstractIntegrationTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);

	@Override
	protected JdbcDatabaseContainer<?> getBackend() {
		return POSTGRES;
	}

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		Connection backendConnection = backendConnection(POSTGRES);
		runScript(backendConnection, "postgres/northwind.sql");
		runScript(backendConnection, "postgres/employee.sql");
	}

	@Test
	void testSimpleStatement() throws Exception {
		testSimpleStatement(POSTGRES, "SELECT * FROM orders");
		Config config = bootstrapConfig();
		String cacheKeyPattern = Driver.keyBuilder(config, Driver.KEYSPACE_CACHE).create("*");
		List<String> keys = redisConnection.sync().keys(cacheKeyPattern);
		Assertions.assertEquals(1, keys.size());
		testSimpleStatement(POSTGRES, "SELECT * FROM employees");
		keys = redisConnection.sync().keys(cacheKeyPattern);
		Assertions.assertEquals(2, keys.size());
		testSimpleStatement(POSTGRES, "SELECT * FROM products");
		keys = redisConnection.sync().keys(cacheKeyPattern);
		Assertions.assertEquals(3, keys.size());
	}

	@Test
	void testUpdateAndGetResultSet() throws Exception {
		testUpdateAndGetResultSet(POSTGRES, "SELECT * FROM orders");
	}

	@Test
	void testPreparedStatement() throws Exception {
		testPreparedStatement(POSTGRES, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@Test
	void testSimpleCallableStatement() throws Exception {
		testCallableStatement(POSTGRES, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@Test
	void testCallableStatementParams() throws Exception {
		String runFunction = "{ ? = call hello( ? ) }";
		try (Connection connection = connection(POSTGRES);
				Statement statement = connection.createStatement();
				CallableStatement callableStatement = connection.prepareCall(runFunction)) {
			callableStatement.registerOutParameter(1, Types.VARCHAR);
			callableStatement.setString(2, "julien");
			callableStatement.executeUpdate();
			Assertions.assertEquals("hello julien", callableStatement.getString(1));
		}
	}

	@Test
	void testCallableStatementRefCursor() throws Exception {
		String runFunction = "{? = call getUsers()}";
		try (Connection connection = connection(POSTGRES);
				Statement statement = connection.createStatement();
				CallableStatement cs = connection.prepareCall(runFunction)) {
			// We must be inside a transaction for cursors to work.
			connection.setAutoCommit(false);
			// register output
			cs.registerOutParameter(1, Types.REF_CURSOR);
			// run function
			cs.execute();
			// get refcursor and convert it to ResultSet
			ResultSet resultSet = (ResultSet) cs.getObject(1);
			while (resultSet.next()) {
				Assertions.assertEquals("test", resultSet.getString("usename"));
				Assertions.assertEquals("********", resultSet.getString("passwd"));
			}

		}
	}

	@Test
	void testCallableStatementGetResultSet() throws Exception {
		testCallableStatementGetResultSet(POSTGRES, "SELECT * FROM orders WHERE employee_id = 8");
	}

	@Test
	void testResultSetMetadata() throws Exception {
		testResultSetMetaData(POSTGRES, "SELECT * FROM orders");
	}

	@Test
	void testConnect() throws SQLException, IOException {
		Config config = new Config();
		config.getDriver().setClassName(POSTGRES.getDriverClassName());
		config.getDriver().setUrl(POSTGRES.getJdbcUrl());
		Properties info = Driver.properties(config);
		info.setProperty("user", POSTGRES.getUsername());
		info.setProperty("password", POSTGRES.getPassword());
		java.sql.Driver driver = DriverManager.getDriver("jdbc:" + redis.getRedisURI());
		Connection connection = driver.connect("jdbc:" + redis.getRedisURI(), info);
		Assertions.assertInstanceOf(SmartConnection.class, connection);
	}

}
