package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.smartcache.Driver;
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.Mappers;
import com.redis.smartcache.core.RuleConfig;
import com.redis.smartcache.core.config.Config;

import io.airlift.units.Duration;

@SuppressWarnings("unchecked")
abstract class AbstractIntegrationTests extends AbstractTests {

    private static final Logger log = Logger.getLogger(AbstractIntegrationTests.class.getName());

    private static final int BUFFER_SIZE = 50000000;

    private Driver driver;

    @BeforeEach
    void setupDriver() {
        driver = new Driver();
    }

    @AfterEach
    void teardownDriver() throws Exception {
        Driver.clear();
    }

    protected static void runScript(Connection backendConnection, String script) throws SQLException, IOException {
        ScriptRunner scriptRunner = new ScriptRunner(backendConnection);
        scriptRunner.setAutoCommit(false);
        scriptRunner.setStopOnError(true);
        try (InputStream inputStream = AbstractIntegrationTests.class.getClassLoader().getResourceAsStream(script)) {
            scriptRunner.runScript(new InputStreamReader(inputStream));
        }
    }

    abstract protected JdbcDatabaseContainer<?> getBackend();

    protected static Connection backendConnection(JdbcDatabaseContainer<?> backend) throws SQLException {
        try {
            Class.forName(backend.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not initialize driver", e);
        }
        return DriverManager.getConnection(backend.getJdbcUrl(), backend.getUsername(), backend.getPassword());
    }

    /**
     * 
     * @param database
     * @param properties the properties tuples (key,value,key,value,...).
     * @return
     * @throws SQLException
     * @throws IOException
     */
    protected SmartConnection smartConnection(JdbcDatabaseContainer<?> database, Consumer<Config>... configurers)
            throws IOException, SQLException {
        Config config = bootstrapConfig();
        config.getDriver().setClassName(database.getDriverClassName());
        config.getDriver().setUrl(database.getJdbcUrl());
        config.getRuleset().setRules(RuleConfig.passthrough().ttl(Duration.valueOf("300s")).build());
        for (Consumer<Config> configurer : configurers) {
            configurer.accept(config);
        }
        Properties info = Mappers.properties(config);
        info.setProperty("user", database.getUsername());
        info.setProperty("password", database.getPassword());
        return driver.connect("jdbc:" + redis.getRedisURI(), info);
    }

    protected static Config bootstrapConfig() {
        Config config = new Config();
        config.getCache().setCodecBufferSizeInBytes(BUFFER_SIZE);
        config.getMetrics().setEnabled(false);
        config.getRuleset().setRules(RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build());
        return config;
    }

    private static interface StatementExecutor {

        ResultSet execute(Connection connection) throws SQLException;

    }

    protected void testSimpleStatement(String sql, JdbcDatabaseContainer<?> databaseContainer, Consumer<Config>... configurers)
            throws Exception {
        test(databaseContainer, c -> c.createStatement().executeQuery(sql), configurers);
    }

    protected void testUpdateAndGetResultSet(JdbcDatabaseContainer<?> databaseContainer, String sql) throws Exception {
        test(databaseContainer, c -> {
            Statement statement = c.createStatement();
            statement.execute(sql);
            return statement.getResultSet();
        });
    }

    protected void testPreparedStatement(JdbcDatabaseContainer<?> databaseContainer, String sql, Object... parameters)
            throws Exception {
        test(databaseContainer, c -> {
            PreparedStatement statement = c.prepareStatement(sql);
            for (int index = 0; index < parameters.length; index++) {
                statement.setObject(index + 1, parameters[index]);
            }
            return statement.executeQuery();
        });
    }

    protected boolean execute(String sql, JdbcDatabaseContainer<?> databaseContainer) throws Exception {
        try (SmartConnection connection = smartConnection(databaseContainer);
                Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }

    protected void testCallableStatement(JdbcDatabaseContainer<?> databaseContainer, String sql, Object... parameters)
            throws Exception {
        test(databaseContainer, c -> {
            CallableStatement statement = c.prepareCall(sql);
            for (int index = 0; index < parameters.length; index++) {
                statement.setObject(index + 1, parameters[index]);
            }
            return statement.executeQuery();
        });
    }

    protected void testCallableStatementGetResultSet(JdbcDatabaseContainer<?> databaseContainer, String sql,
            Object... parameters) throws Exception {
        test(databaseContainer, c -> {
            CallableStatement statement = c.prepareCall(sql);
            statement.execute();
            return statement.getResultSet();
        });
    }

    private <T extends Statement> void test(JdbcDatabaseContainer<?> databaseContainer, StatementExecutor executor,
            Consumer<Config>... configurers) throws Exception {
        try (Connection backendConnection = backendConnection(databaseContainer);
                SmartConnection smartConnection = smartConnection(databaseContainer, configurers)) {
            Utils.assertEquals(executor.execute(backendConnection), executor.execute(smartConnection));
            awaitUntil(() -> {
                try {
                    executor.execute(smartConnection);
                } catch (Exception e) {
                    log.log(Level.SEVERE, "Could not execute statement", e);
                }
                String keyPattern = KeyBuilder.of(new Config()).sub(Driver.KEYSPACE_CACHE).build("*");
                return !redisConnection.sync().keys(keyPattern).isEmpty();
            });
            Utils.assertEquals(executor.execute(backendConnection), executor.execute(smartConnection));
        }
    }

    protected void testResultSetMetaData(String sql, JdbcDatabaseContainer<?> databaseContainer) throws Exception {
        try (SmartConnection connection = smartConnection(databaseContainer);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            Assertions.assertNotNull(resultSet);
            final ResultSetMetaData metaData = resultSet.getMetaData();
            Assertions.assertNotNull(metaData);
            int colCount = metaData.getColumnCount();
            Assertions.assertTrue(colCount > 0);
            for (int i = 1; i <= colCount; i++) {
                Assertions.assertNotNull(metaData.getColumnName(i));
                Assertions.assertNotNull(metaData.getColumnLabel(i));
                Assertions.assertNotNull(metaData.getColumnTypeName(i));
                Assertions.assertNotNull(metaData.getCatalogName(i));
                Assertions.assertNotNull(metaData.getColumnClassName(i));
                Assertions.assertTrue(metaData.getColumnDisplaySize(i) > 0);
                Assertions.assertNotNull(metaData.getSchemaName(i));
                Assertions.assertNotNull(metaData.getTableName(i));
            }
        }
    }

}
