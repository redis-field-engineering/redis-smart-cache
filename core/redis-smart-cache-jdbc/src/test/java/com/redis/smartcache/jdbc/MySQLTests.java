package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

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
}
