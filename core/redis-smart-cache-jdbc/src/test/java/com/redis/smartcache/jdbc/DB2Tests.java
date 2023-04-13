package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;

@EnabledOnOs(OS.LINUX)
class DB2Tests extends AbstractIntegrationTests {

	@SuppressWarnings("deprecation")
	@Container
	private static final Db2Container DB2 = new Db2Container().acceptLicense();

	@Override
	protected JdbcDatabaseContainer<?> getBackend() {
		return DB2;
	}

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		Connection backendConnection = backendConnection(DB2);
		runScript(backendConnection, "db2/create.sql");
		runScript(backendConnection, "db2/data.sql");
	}

	@Test
	void testSimpleStatement() throws Exception {
		testSimpleStatement("SELECT * FROM books", DB2);
	}

	@Test
	void testUpdateAndGetResultSet() throws Exception {
		testUpdateAndGetResultSet(DB2, "SELECT * FROM books");
	}

	@Test
	void testPreparedStatement() throws Exception {
		testPreparedStatement(DB2, "SELECT * FROM books WHERE publisher_id = ?", 5);
	}

	@Test
	void testCallableStatementGetResultSet() throws Exception {
		testCallableStatementGetResultSet(DB2, "SELECT * FROM books WHERE publisher_id = 5");
	}

	@Test
	void testResultSetMetadata() throws Exception {
		testResultSetMetaData("SELECT * FROM books", DB2);
	}
}
