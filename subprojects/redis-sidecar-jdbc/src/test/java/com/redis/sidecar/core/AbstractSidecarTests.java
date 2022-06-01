package com.redis.sidecar.core;

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
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.sidecar.Driver;
import com.redis.sidecar.core.Config.ByteSize;
import com.redis.sidecar.core.Config.Redis;
import com.redis.sidecar.jdbc.SidecarConnection;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

public abstract class AbstractSidecarTests extends AbstractTestcontainersRedisTestBase {

	private static final int BUFFER_SIZE = ByteSize.ofMB(300).toBytes();

	private final RedisModulesContainer redis = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
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

	protected Connection connection(JdbcDatabaseContainer<?> database, RedisTestContext redis) throws SQLException {
		Config config = config(redis);
		try {
			return new SidecarConnection(connection(database), redis.getClient(), config,
					ConfigUpdater.create(redis.getClient(), config), Driver.meterRegistry(config, redis.getClient()));
		} catch (JsonProcessingException e) {
			throw new SQLException("Could not initialize ResultSet cache", e);
		}
	}

	protected Config config(RedisTestContext redis) {
		Config config = new Config();
		config.setBufferSize(BUFFER_SIZE);
		config.getMetrics().setPublishInterval(1);
		Redis redisConfig = new Redis();
		redisConfig.setCluster(redis.isCluster());
		redisConfig.setUri(redis.getRedisURI());
		config.setRedis(redisConfig);
		return config;
	}

	private static interface StatementExecutor {

		ResultSet execute(Connection connection) throws SQLException;

	}

	protected void testSimpleStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws SQLException {
		test(databaseContainer, redis, c -> c.createStatement().executeQuery(sql));
	}

	protected void testUpdateAndGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql) throws SQLException {
		test(databaseContainer, redis, c -> {
			Statement statement = c.createStatement();
			statement.execute(sql);
			return statement.getResultSet();
		});
	}

	protected void testPreparedStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws SQLException {
		test(databaseContainer, redis, c -> {
			PreparedStatement statement = c.prepareStatement(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws SQLException {
		test(databaseContainer, redis, c -> {
			CallableStatement statement = c.prepareCall(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatementGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql, Object... parameters) throws SQLException {
		test(databaseContainer, redis, c -> {
			CallableStatement statement = c.prepareCall(sql);
			statement.execute();
			return statement.getResultSet();
		});
	}

	private <T extends Statement> void test(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			StatementExecutor executor) throws SQLException {
		try (Connection databaseConnection = connection(databaseContainer);
				Connection connection = connection(databaseContainer, redis)) {
			TestUtils.assertEquals(executor.execute(databaseConnection), executor.execute(connection));
			ResultSet resultSet = null;
			int count = 100;
			for (int index = 0; index < count; index++) {
				resultSet = executor.execute(connection);
			}
			TestUtils.assertEquals(executor.execute(databaseConnection), resultSet);
		}
	}

	protected void testResultSetMetaData(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws SQLException {
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
