package com.redis.sidecar.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;

import com.redis.sidecar.core.AbstractSidecarTests;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class DB2Tests extends AbstractSidecarTests {

	@SuppressWarnings("deprecation")
	@Container
	private static final Db2Container DB2 = new Db2Container().acceptLicense();

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(DB2);
		runScript(backendConnection, "db2/create.sql");
		runScript(backendConnection, "db2/data.sql");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(DB2, redis, "SELECT * FROM books");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(DB2, redis, "SELECT * FROM books");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(DB2, redis, "SELECT * FROM books WHERE publisher_id = ?", 5);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(DB2, redis, "SELECT * FROM books WHERE publisher_id = 5");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(DB2, redis, "SELECT * FROM books");
	}
}
