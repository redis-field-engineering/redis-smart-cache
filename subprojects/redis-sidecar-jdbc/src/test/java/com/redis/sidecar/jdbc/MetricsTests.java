package com.redis.sidecar.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.sidecar.Driver;
import com.redis.sidecar.core.AbstractSidecarTests;
import com.redis.sidecar.core.Config;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.RedisCommandExecutionException;

class MetricsTests extends AbstractSidecarTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(POSTGRESQL);
		runScript(backendConnection, "postgres/northwind.sql");
		runScript(backendConnection, "postgres/employee.sql");
	}

	@Override
	protected Config config(RedisTestContext redis) {
		Config config = super.config(redis);
		config.getMetrics().setPublishInterval(1);
		return config;
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		SidecarConnection connection = connection(POSTGRESQL, redis);
		int count = Integer.parseInt(System.getProperty(Driver.PROPERTY_PREFIX + ".test-iterations", "100"));
		Random random = new Random();
		for (int index = 0; index < count; index++) {
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM orders WHERE employee_id = ?");
			statement.setObject(1, 1 + random.nextInt(9));
			statement.executeQuery();
		}
		RedisModulesCommands<String, String> sync = redis.sync();
		Awaitility.await().until(() -> {
			try {
				return sync.tsGet("sidecar:default:gets:latency:mean") != null;
			} catch (RedisCommandExecutionException e) {
				return false;
			}
		});
	}

}
