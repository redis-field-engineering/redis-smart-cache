package com.redis.sidecar;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class PostgreSQLTests extends AbstractSidecarTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>(POSTGRE_DOCKER_IMAGE_NAME);

	@BeforeAll
	public void setupAll() throws SQLException, IOException {
		Connection backendConnection = connection(POSTGRESQL);
		runScript(backendConnection, "postgres-northwind.sql");
		runScript(backendConnection, "postgres-employee.sql");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSimpleStatement(RedisTestContext redis) throws Exception {
		testSimpleStatement(POSTGRESQL, redis, "SELECT * FROM orders");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testUpdateAndGetResultSet(RedisTestContext redis) throws Exception {
		testUpdateAndGetResultSet(POSTGRESQL, redis, "SELECT * FROM orders");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testPreparedStatement(RedisTestContext redis) throws Exception {
		testPreparedStatement(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatement(RedisTestContext redis) throws Exception {
		testCallableStatement(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = ?", 8);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementParams(RedisTestContext redis) throws Exception {
		try (Connection connection = connection(POSTGRESQL, redis)) {
			connection.createStatement().execute(generateInsert("julien", new BigDecimal(999.80)));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatement2(RedisTestContext redis) throws SQLException {
		String runFunction = "{ ? = call hello( ? ) }";

		try (Connection conn = connection(POSTGRESQL, redis);
				Statement statement = conn.createStatement();
				CallableStatement callableStatement = conn.prepareCall(runFunction)) {
			callableStatement.registerOutParameter(1, Types.VARCHAR);
			callableStatement.setString(2, "julien");
			callableStatement.executeUpdate();
			Assertions.assertEquals("hello julien", callableStatement.getString(1));
		}
	}

	private static String generateInsert(String name, BigDecimal salary) {
		return "INSERT INTO EMPLOYEE (NAME, SALARY, CREATED_DATE) " + "VALUES ('" + name + "','" + salary + "','"
				+ LocalDateTime.now() + "')";

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementRefCursor(RedisTestContext redis) throws SQLException {

		String runFunction = "{? = call getUsers()}";

		try (Connection conn = connection(POSTGRESQL, redis);
				Statement statement = conn.createStatement();
				CallableStatement cs = conn.prepareCall(runFunction)) {

			// We must be inside a transaction for cursors to work.
			conn.setAutoCommit(false);

			// register output
			cs.registerOutParameter(1, Types.REF_CURSOR);

			// run function
			cs.execute();

			// get refcursor and convert it to ResultSet
			ResultSet resultSet = (ResultSet) cs.getObject(1);
			while (resultSet.next()) {
				Assertions.assertEquals("test", resultSet.getString("usename"));
				Assertions.assertEquals("********", resultSet.getString("passwd"));
			}

		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testCallableStatementGetResultSet(RedisTestContext redis) throws Exception {
		testCallableStatementGetResultSet(POSTGRESQL, redis, "SELECT * FROM orders WHERE employee_id = 8");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testResultSetMetadata(RedisTestContext redis) throws Exception {
		testResultSetMetaData(POSTGRESQL, redis, "SELECT * FROM orders");
	}

}
