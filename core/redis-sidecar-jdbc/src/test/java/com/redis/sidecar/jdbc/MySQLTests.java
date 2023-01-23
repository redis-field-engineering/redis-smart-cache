package com.redis.sidecar.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.MySQLContainer;

import com.redis.sidecar.AbstractSidecarTests;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class MySQLTests extends AbstractSidecarTests {

	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>(MySQLContainer.NAME);

	@BeforeAll
	protected void setupDatabaseContainer() throws SQLException, IOException {
		Assumptions.assumeTrue(RedisServer.isEnabled("MYSQL"));
		MYSQL.start();
		Connection backendConnection = connection(MYSQL);
		runScript(backendConnection, "mysql/northwind.sql");
	}

	@AfterAll
	protected void teardownDatabaseContainer() {
		MYSQL.stop();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(MYSQL, redis, "SELECT * FROM Product");
		testSimpleStatement(MYSQL, redis, "SELECT * FROM Category");
		testSimpleStatement(MYSQL, redis, "SELECT * FROM Supplier");
		testSimpleStatement(MYSQL, redis, "SELECT * FROM SalesOrder");
		testSimpleStatement(MYSQL, redis, "SELECT * FROM OrderDetail");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(MYSQL, redis, "SELECT * FROM Employee");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(MYSQL, redis, "SELECT * FROM Employee WHERE mgrid = ?", 5);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(MYSQL, redis, "SELECT * FROM Employee WHERE mgrid = 5");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(MYSQL, redis, "SELECT * FROM Employee");
	}
}
