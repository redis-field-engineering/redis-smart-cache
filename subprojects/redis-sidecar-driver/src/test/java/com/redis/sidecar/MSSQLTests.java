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
	private static final MSSQLServerContainer<?> MSSQL = new MSSQLServerContainer<>(MSSQLServerContainer.IMAGE)
			.acceptLicense();

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(MSSQL);
		runScript(backendConnection, "mssql/create_tables.sql");
		runScript(backendConnection, "mssql/populate_tables.sql");
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
