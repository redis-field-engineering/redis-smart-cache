package com.redis.sidecar;

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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

public abstract class AbstractSidecarTests extends AbstractTestcontainersRedisTestBase {

	private static final Logger log = Logger.getLogger(AbstractSidecarTests.class.getName());

	private static final int BUFFER_SIZE = 50000000;

	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));
	private final PropertiesMapper propsMapper = new PropertiesMapper();

	private SidecarDriver sidecarDriver;
	private Duration testTimeout = Duration.ofHours(1);

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
	}

	@BeforeAll
	private void setupSidecarDriver() {
		sidecarDriver = new SidecarDriver();
	}

	@AfterAll
	private void teardownSidecarDriver() {
		if (sidecarDriver != null) {
			sidecarDriver.clear();
		}
	}

	protected void runScript(Connection backendConnection, String script) throws SQLException, IOException {
		ScriptRunner scriptRunner = new ScriptRunner(backendConnection);
		scriptRunner.setAutoCommit(false);
		scriptRunner.setStopOnError(true);
		try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(script)) {
			scriptRunner.runScript(new InputStreamReader(inputStream));
		}
	}

	protected Connection connection(JdbcDatabaseContainer<?> container) throws SQLException {
		try {
			Class.forName(container.getDriverClassName());
		} catch (ClassNotFoundException e) {
			throw new SQLException("Could not initialize driver", e);
		}
		return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
	}

	protected Connection connection(JdbcDatabaseContainer<?> database, RedisTestContext redis)
			throws SQLException, IOException {
		BootstrapConfig config = new BootstrapConfig();
		config.getRedis().setCodecBufferSize(BUFFER_SIZE);
		config.getRedis().setCluster(redis.isCluster());
		config.getDriver().setClassName(database.getDriverClassName());
		config.getDriver().setUrl(database.getJdbcUrl());
		config.setConfigStep(3600);
		Properties info = propsMapper.write(config);
		info.put("user", database.getUsername());
		info.put("password", database.getPassword());
		return sidecarDriver.connect("jdbc:" + redis.getRedisURI(), info);
	}

	private static interface StatementExecutor {

		ResultSet execute(Connection connection) throws SQLException;

	}

	protected void testSimpleStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws Exception {
		test(databaseContainer, redis, c -> c.createStatement().executeQuery(sql));
	}

	protected void testUpdateAndGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql) throws Exception {
		test(databaseContainer, redis, c -> {
			Statement statement = c.createStatement();
			statement.execute(sql);
			return statement.getResultSet();
		});
	}

	protected void testPreparedStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws Exception {
		test(databaseContainer, redis, c -> {
			PreparedStatement statement = c.prepareStatement(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws Exception {
		test(databaseContainer, redis, c -> {
			CallableStatement statement = c.prepareCall(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatementGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql, Object... parameters) throws Exception {
		test(databaseContainer, redis, c -> {
			CallableStatement statement = c.prepareCall(sql);
			statement.execute();
			return statement.getResultSet();
		});
	}

	private <T extends Statement> void test(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			StatementExecutor executor) throws Exception {
		try (Connection databaseConnection = connection(databaseContainer);
				Connection connection = connection(databaseContainer, redis)) {
			TestUtils.assertEquals(executor.execute(databaseConnection), executor.execute(connection));
			Awaitility.await().timeout(testTimeout).until(() -> {
				try {
					executor.execute(connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not execute statement", e);
				}
				return !redis.sync().keys("sidecar:cache:*").isEmpty();
			});
			TestUtils.assertEquals(executor.execute(databaseConnection), executor.execute(databaseConnection));
		}
	}

	protected void testResultSetMetaData(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws Exception {
		try (Connection connection = connection(databaseContainer, redis)) {
			Statement statement = connection.createStatement();
			statement.execute(sql);
			ResultSet resultSet = statement.getResultSet();
			Assert.assertNotNull(resultSet);
			final ResultSetMetaData metaData = resultSet.getMetaData();
			Assert.assertNotNull(metaData);
			int colCount = metaData.getColumnCount();
			Assert.assertTrue(colCount > 0);
			for (int i = 1; i <= colCount; i++) {
				Assert.assertNotNull(metaData.getColumnName(i));
				Assert.assertNotNull(metaData.getColumnLabel(i));
				Assert.assertNotNull(metaData.getColumnTypeName(i));
				Assert.assertNotNull(metaData.getCatalogName(i));
				Assert.assertNotNull(metaData.getColumnClassName(i));
				Assert.assertTrue(metaData.getColumnDisplaySize(i) > 0);
				Assert.assertNotNull(metaData.getSchemaName(i));
				Assert.assertNotNull(metaData.getTableName(i));
			}
		}
	}

}