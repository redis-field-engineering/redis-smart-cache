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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@EnabledOnOs(OS.LINUX)
@SuppressWarnings("unchecked")
class OracleTests extends AbstractIntegrationTests {

    private static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse("gvenzl/oracle-xe")
            .withTag("18.4.0-slim");

    @Container
    private static final OracleContainer ORACLE = new OracleContainer(ORACLE_DOCKER_IMAGE_NAME);

    @Override
    protected JdbcDatabaseContainer<?> getBackend() {
        return ORACLE;
    }

    @BeforeAll
    public static void setupAll() throws SQLException, IOException {
        Connection backendConnection = backendConnection(ORACLE);
        runScript(backendConnection, "oracle/hr.sql");
        runScript(backendConnection, "oracle/employee.sql");
    }

    @Test
    void testSimpleStatement() throws Exception {
        testSimpleStatement("SELECT * FROM employees", ORACLE);
        testSimpleStatement("SELECT * FROM emp_details_view", ORACLE);
        testSimpleStatement("SELECT * FROM locations", ORACLE);
    }

    @Test
    void testUpdateAndGetResultSet() throws Exception {
        testUpdateAndGetResultSet(ORACLE, "SELECT * FROM employees");
    }

    @Test
    void testPreparedStatement() throws Exception {
        testPreparedStatement(ORACLE, "SELECT * FROM employees WHERE department_id = ?", 30);
    }

    @Test
    void testCallableStatement() throws Exception {
        try (Connection connection = smartConnection(ORACLE)) {
            CallableStatement callableStatement = connection.prepareCall("{ call insert_employee(?,?,?) }");
            callableStatement.setString(1, "julien");
            callableStatement.setBigDecimal(2, new BigDecimal("99.99"));
            callableStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            Assertions.assertEquals(1, callableStatement.executeUpdate());
        }
    }

    @Test
    void testCallableStatementGetResultSet() throws Exception {
        testCallableStatementGetResultSet(ORACLE, "SELECT * FROM employees WHERE department_id = 30");
    }

    @Test
    void testResultSetMetadata() throws Exception {
        testResultSetMetaData("SELECT * FROM employees", ORACLE);
    }

}
