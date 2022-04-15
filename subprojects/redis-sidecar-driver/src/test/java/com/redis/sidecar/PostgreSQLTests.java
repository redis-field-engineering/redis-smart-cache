package com.redis.sidecar;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@Testcontainers
class PostgreSQLTests extends AbstractSidecarTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);
	private static Connection backendConnection;

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		backendConnection = getDatabaseConnection(POSTGRESQL);
		ScriptRunner scriptRunner = new ScriptRunner(backendConnection);
		scriptRunner.setAutoCommit(false);
		scriptRunner.setStopOnError(true);
		String file = "northwind.sql";
		try (InputStream inputStream = PostgreSQLTests.class.getClassLoader().getResourceAsStream(file)) {
			scriptRunner.runScript(new InputStreamReader(inputStream));
		}
	}

	@Override
	protected Properties properties(JdbcDatabaseContainer<?> container) {
		Properties info = new Properties();
		info.setProperty("user", container.getUsername());
		info.setProperty("password", container.getPassword());
		return info;
	}

	@AfterAll
	public void teardownAll() throws SQLException {
		backendConnection.close();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(POSTGRESQL, redis, "SELECT * FROM orders");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(POSTGRESQL, redis, "SELECT * FROM orders");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatement(RedisTestContext redis) throws Exception {
		testCallableStatement(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = 8");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(POSTGRESQL, redis.getServer(), "SELECT * FROM orders");
	}

}
