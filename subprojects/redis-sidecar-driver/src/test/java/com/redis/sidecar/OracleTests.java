package com.redis.sidecar;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class OracleTests extends AbstractSidecarTests {

	private static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse("gvenzl/oracle-xe")
			.withTag("18.4.0-slim");

	@Container
	private static final OracleContainer ORACLE = new OracleContainer(ORACLE_DOCKER_IMAGE_NAME);

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(ORACLE);
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
		String createTable = "CREATE TABLE EMPLOYEE" + "("
				+ " ID number GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),"
				+ " NAME varchar2(100) NOT NULL," + " SALARY number(15, 2) NOT NULL,"
				+ " CREATED_DATE DATE DEFAULT SYSDATE NOT NULL," + " CONSTRAINT employee_pk PRIMARY KEY (ID)" + ")";
		String createSP = "CREATE OR REPLACE PROCEDURE insert_employee( p_name IN EMPLOYEE.NAME%TYPE, "
				+ " p_salary IN EMPLOYEE.SALARY%TYPE, " + " p_date IN EMPLOYEE.CREATED_DATE%TYPE) " + " AS " + " BEGIN "
				+ "     INSERT INTO EMPLOYEE (\"NAME\", \"SALARY\", \"CREATED_DATE\") VALUES (p_name, p_salary, p_date); "
				+ "     COMMIT; " + " END; ";
		String runSP = "{ call insert_employee(?,?,?) }";
		try (Connection connection = connection(ORACLE, redis)) {
			connection.createStatement().execute(createTable);
			connection.createStatement().execute(createSP);
			CallableStatement callableStatement = connection.prepareCall(runSP);
			callableStatement.setString(1, "mkyong");
			callableStatement.setBigDecimal(2, new BigDecimal("99.99"));
			callableStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			callableStatement.executeUpdate();
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
