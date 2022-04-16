package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@Testcontainers
class OracleTests extends AbstractSidecarTests {

	private static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse("gvenzl/oracle-xe")
			.withTag("18.4.0-slim");

	@Container
	private static final OracleContainer ORACLE = new OracleContainer(ORACLE_DOCKER_IMAGE_NAME);

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = getDatabaseConnection(ORACLE);
		runScript(backendConnection, "hr.sql");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(ORACLE, redis, "SELECT * FROM employees");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(ORACLE, redis, "SELECT * FROM employees");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(ORACLE, redis, "SELECT * FROM employees WHERE department_id = ?", 30);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatement(RedisTestContext redis) throws Exception {
		testCallableStatement(ORACLE, redis, "SELECT * FROM employees WHERE department_id = ?", 30);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(ORACLE, redis, "SELECT * FROM employees WHERE department_id = 30");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(ORACLE, redis.getServer(), "SELECT * FROM employees");
	}

}
