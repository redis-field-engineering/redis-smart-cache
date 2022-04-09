package com.redis.sidecar;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@Testcontainers
public class PostgreSQLTests extends AbstractSidecarTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);
	private static Connection backendConnection;

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		backendConnection = getDataSource(POSTGRESQL).getConnection();
		ScriptRunner scriptRunner = new ScriptRunner(backendConnection);
		scriptRunner.setAutoCommit(false);
		scriptRunner.setStopOnError(true);
		String file = "northwind.sql";
		InputStream inputStream = PostgreSQLTests.class.getClassLoader().getResourceAsStream(file);
		if (inputStream == null) {
			throw new FileNotFoundException(file);
		}
		scriptRunner.runScript(new InputStreamReader(inputStream));
	}

	@AfterAll
	public static void teardownAll() throws SQLException {
		backendConnection.close();
	}

	@AfterEach
	void clearTables() throws SQLException {
		try (Statement statement = backendConnection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS mytable");
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCachePut(RedisTestContext redis) throws Exception {
		DataSource sidecarDataSource = getSidecarDataSource(POSTGRESQL, redis);
		String query = "SELECT COUNT(*) AS count FROM orders";
		try (Connection connection = sidecarDataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(query);
				statement.getResultSet();
				Assertions.assertEquals(1, redis.getConnection().sync().keys("SELECT*").size());
			}
		}
	}

}
