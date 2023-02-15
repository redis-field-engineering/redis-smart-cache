package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@EnabledOnOs(OS.LINUX)
class OracleTests extends AbstractIntegrationTests {

	private static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse("gvenzl/oracle-xe")
			.withTag("18.4.0-slim");

	@Container
	private static final OracleContainer ORACLE = new OracleContainer(ORACLE_DOCKER_IMAGE_NAME);

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(ORACLE);
		runScript(backendConnection, "oracle/hr.sql");
		runScript(backendConnection, "oracle/employee.sql");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(ORACLE, redis, "SELECT * FROM employees");
		testSimpleStatement(ORACLE, redis, "SELECT * FROM emp_details_view");
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
		try (Connection connection = connection(ORACLE, redis)) {
			CallableStatement callableStatement = connection.prepareCall("{ call insert_employee(?,?,?) }");
			callableStatement.setString(1, "julien");
			callableStatement.setBigDecimal(2, new BigDecimal("99.99"));
			callableStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			Assertions.assertEquals(1, callableStatement.executeUpdate());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(ORACLE, redis, "SELECT * FROM employees WHERE department_id = 30");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(ORACLE, redis, "SELECT * FROM employees");
	}

}
