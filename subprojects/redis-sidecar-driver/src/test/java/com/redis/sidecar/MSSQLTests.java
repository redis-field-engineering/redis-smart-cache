package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class MSSQLTests extends AbstractSidecarTests {

	@Container
	private static final MSSQLServerContainer<?> MSSQL = new MSSQLServerContainer<>(MSSQLServerContainer.IMAGE).acceptLicense();

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(MSSQL);
		runScript(backendConnection, "mssql/demo_hr_02_create_tables.sql");
		runScript(backendConnection, "mssql/demo_hr_03_populate_tables.sql");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(MSSQL, redis, "SELECT * FROM LOCATIONS");
	}

}
