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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.Driver;
import com.redis.smartcache.Utils;
import com.redis.smartcache.core.Config;
import com.redis.testcontainers.RedisStackContainer;

import io.airlift.units.Duration;

@Testcontainers
abstract class AbstractIntegrationTests {

	private static final Logger log = Logger.getLogger(AbstractIntegrationTests.class.getName());

	private static final int BUFFER_SIZE = 50000000;

	@Container
	protected static final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	private Driver driver;
	private RedisModulesClient client;
	protected StatefulRedisModulesConnection<String, String> redisConnection;

	@BeforeEach
	void setupRedisClient() {
		driver = new Driver();
		client = RedisModulesClient.create(redis.getRedisURI());
		redisConnection = client.connect();
		redisConnection.sync().flushall();
		Awaitility.await().until(() -> redisConnection.sync().dbsize() == 0);
	}

	@AfterEach
	void teardownRedisClient() {
		redisConnection.close();
		client.shutdown();
		client.getResources().shutdown();
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

	protected SmartConnection connection(JdbcDatabaseContainer<?> database) throws SQLException, IOException {
		Config config = bootstrapConfig();
		config.getDriver().setClassName(database.getDriverClassName());
		config.getDriver().setUrl(database.getJdbcUrl());
		config.getMetrics().setEnabled(false);
		config.getRuleset().setRefresh(new Duration(1, TimeUnit.HOURS));
		Properties info = Driver.properties(config);
		info.put("user", database.getUsername());
		info.put("password", database.getPassword());
		return driver.connect("jdbc:" + redis.getRedisURI(), info);
	}

	protected static Config bootstrapConfig() {
		Config config = new Config();
		config.getRedis().setCodecBufferSizeInBytes(BUFFER_SIZE);
		config.getRuleset().setRefresh(new Duration(1, TimeUnit.HOURS));
		return config;
	}

	private static interface StatementExecutor {

		ResultSet execute(Connection connection) throws SQLException;

	}

	protected void testSimpleStatement(JdbcDatabaseContainer<?> databaseContainer, String sql) throws Exception {
		test(databaseContainer, c -> c.createStatement().executeQuery(sql));
	}

	protected void testUpdateAndGetResultSet(JdbcDatabaseContainer<?> databaseContainer, String sql) throws Exception {
		test(databaseContainer, c -> {
			try (Statement statement = c.createStatement()) {
				statement.execute(sql);
				return statement.getResultSet();
			}
		});
	}

	protected void testPreparedStatement(JdbcDatabaseContainer<?> databaseContainer, String sql, Object... parameters)
			throws Exception {
		test(databaseContainer, c -> {
			try (PreparedStatement statement = c.prepareStatement(sql)) {
				for (int index = 0; index < parameters.length; index++) {
					statement.setObject(index + 1, parameters[index]);
				}
				return statement.executeQuery();
			}
		});
	}

	protected boolean execute(JdbcDatabaseContainer<?> databaseContainer, String sql) throws Exception {
		try (SmartConnection connection = connection(databaseContainer);
				Statement statement = connection.createStatement()) {
			return statement.execute(sql);
		}
	}

	protected void testCallableStatement(JdbcDatabaseContainer<?> databaseContainer, String sql, Object... parameters)
			throws Exception {
		test(databaseContainer, c -> {
			try (CallableStatement statement = c.prepareCall(sql)) {
				for (int index = 0; index < parameters.length; index++) {
					statement.setObject(index + 1, parameters[index]);
				}
				return statement.executeQuery();
			}
		});
	}

	protected void testCallableStatementGetResultSet(JdbcDatabaseContainer<?> databaseContainer, String sql,
			Object... parameters) throws Exception {
		test(databaseContainer, c -> {
			try (CallableStatement statement = c.prepareCall(sql)) {
				statement.execute();
				return statement.getResultSet();
			}
		});
	}

	private <T extends Statement> void test(JdbcDatabaseContainer<?> databaseContainer, StatementExecutor executor)
			throws Exception {
		try (Connection databaseConnection = connection(databaseContainer);
				SmartConnection connection = connection(databaseContainer)) {
			Utils.assertEquals(executor.execute(databaseConnection), executor.execute(connection));
			Awaitility.await().until(() -> {
				try {
					executor.execute(connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not execute statement", e);
				}
				String keyPattern = Driver.keyBuilder(new Config(), Driver.KEYSPACE_CACHE).create("*");
				return !redisConnection.sync().keys(keyPattern).isEmpty();
			});
			Utils.assertEquals(executor.execute(databaseConnection), executor.execute(databaseConnection));
		}
	}

	protected void testResultSetMetaData(JdbcDatabaseContainer<?> databaseContainer, String sql) throws Exception {
		try (Connection connection = connection(databaseContainer);
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
