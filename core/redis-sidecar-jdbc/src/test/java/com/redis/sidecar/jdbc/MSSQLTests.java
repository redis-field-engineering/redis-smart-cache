package com.redis.sidecar.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.MSSQLServerContainer;

import com.redis.sidecar.AbstractSidecarTests;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class MSSQLTests extends AbstractSidecarTests {

	private static final MSSQLServerContainer<?> MSSQL = new MSSQLServerContainer<>(MSSQLServerContainer.IMAGE)
			.acceptLicense();

	@BeforeAll
	protected void setupDatabaseContainer() throws SQLException, IOException {
		Assumptions.assumeTrue(RedisServer.isEnabled("MSSQL"));
		MSSQL.start();
		Connection backendConnection = connection(MSSQL);
		runScript(backendConnection, "mssql/create_tables.sql");
		runScript(backendConnection, "mssql/populate_tables.sql");
	}

	@AfterAll
	protected void teardownDatabaseContainer() {
		MSSQL.stop();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(MSSQL, redis, "SELECT * FROM LOCATIONS");
		testSimpleStatement(MSSQL, redis, "SELECT * FROM DEPARTMENTS");
		testSimpleStatement(MSSQL, redis, "SELECT * FROM JOB_HISTORY");
		testSimpleStatement(MSSQL, redis, "SELECT * FROM EMPLOYEES");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(MSSQL, redis, "SELECT * FROM LOCATIONS");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(MSSQL, redis, "SELECT * FROM LOCATIONS WHERE COUNTRY_ID = ?", "US");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(MSSQL, redis, "SELECT * FROM LOCATIONS WHERE COUNTRY_ID = 'US'");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(MSSQL, redis, "SELECT * FROM LOCATIONS");
	}
}
