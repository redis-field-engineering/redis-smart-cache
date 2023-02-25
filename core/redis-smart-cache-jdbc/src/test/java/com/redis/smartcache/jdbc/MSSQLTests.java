package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;

@EnabledOnOs(OS.LINUX)
class MSSQLTests extends AbstractIntegrationTests {

	@Container
	private static final MSSQLServerContainer<?> MSSQL = new MSSQLServerContainer<>(MSSQLServerContainer.IMAGE)
			.acceptLicense();

	@Override
	protected JdbcDatabaseContainer<?> getBackend() {
		return MSSQL;
	}

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		Connection backendConnection = backendConnection(MSSQL);
		runScript(backendConnection, "mssql/create_tables.sql");
		runScript(backendConnection, "mssql/populate_tables.sql");
	}

	@Test
	void testSimpleStatement() throws Exception {
		testSimpleStatement(MSSQL, "SELECT * FROM LOCATIONS");
		testSimpleStatement(MSSQL, "SELECT * FROM DEPARTMENTS");
		testSimpleStatement(MSSQL, "SELECT * FROM JOB_HISTORY");
		testSimpleStatement(MSSQL, "SELECT * FROM EMPLOYEES");
	}

	@Test
	void testUpdateAndGetResultSet() throws Exception {
		testUpdateAndGetResultSet(MSSQL, "SELECT * FROM LOCATIONS");
	}

	@Test
	void testPreparedStatement() throws Exception {
		testPreparedStatement(MSSQL, "SELECT * FROM LOCATIONS WHERE COUNTRY_ID = ?", "US");
	}

	@Test
	void testCallableStatementGetResultSet() throws Exception {
		testCallableStatementGetResultSet(MSSQL, "SELECT * FROM LOCATIONS WHERE COUNTRY_ID = 'US'");
	}

	@Test
	void testResultSetMetadata() throws Exception {
		testResultSetMetaData(MSSQL, "SELECT * FROM LOCATIONS");
	}
}
