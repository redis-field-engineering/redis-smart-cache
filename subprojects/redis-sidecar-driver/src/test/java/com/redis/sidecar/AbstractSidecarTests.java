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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.sidecar.impl.RedisClusterResultSetCache;
import com.redis.sidecar.impl.RedisResultSetCache;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.api.StatefulConnection;

abstract class AbstractSidecarTests extends AbstractTestcontainersRedisTestBase {

	private final RedisContainer redis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	private final RedisClusterContainer redisCluster = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
					.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis, redisCluster);
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

	protected SidecarConnection connection(JdbcDatabaseContainer<?> database, RedisTestContext redis)
			throws SQLException {
		Connection connection = connection(database);
		return new SidecarConnection(connection, cache(redis));
	}

	private ResultSetCache cache(RedisTestContext redis) {
		GenericObjectPoolConfig<StatefulConnection<String, byte[]>> poolConfig = new GenericObjectPoolConfig<>();
		if (redis.isCluster()) {
			return new RedisClusterResultSetCache(redis.getRedisClusterClient(), poolConfig);
		}
		return new RedisResultSetCache(redis.getRedisClient(), poolConfig);
	}

	protected void assertEquals(ResultSet expected, ResultSet actual) throws SQLException {
		ResultSetMetaData expectedMeta = expected.getMetaData();
		ResultSetMetaData actualMeta = actual.getMetaData();
		Assertions.assertEquals(expectedMeta.getColumnCount(), actualMeta.getColumnCount());
		for (int index = 0; index < expectedMeta.getColumnCount(); index++) {
			Assertions.assertEquals(expectedMeta.getColumnName(index + 1), actualMeta.getColumnName(index + 1));
			Assertions.assertEquals(expectedMeta.getColumnType(index + 1), actualMeta.getColumnType(index + 1));
		}
		int count = 0;
		while (expected.next()) {
			Assertions.assertTrue(actual.next());
			for (int index = 1; index <= expectedMeta.getColumnCount(); index++) {
				Object expectedValue = normalize(expected.getObject(index));
				Object actualValue = normalize(actual.getObject(index));
				Assertions.assertEquals(expectedValue, actualValue, String.format("Column %s type %s",
						expectedMeta.getColumnName(index), expectedMeta.getColumnType(index)));
			}
			if (expected.getType() != ResultSet.TYPE_FORWARD_ONLY && actual.getType() != ResultSet.TYPE_FORWARD_ONLY) {
				Assertions.assertEquals(expected.isLast(), actual.isLast());
			}
			count++;
		}
		Assertions.assertTrue(count > 0);
	}

	private Object normalize(Object value) {
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		if (value instanceof Timestamp) {
			return ((Timestamp) value).toLocalDateTime();
		}
		return value;
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
				SidecarConnection connection = connection(databaseContainer, redis)) {
			assertEquals(executor.execute(databaseConnection), executor.execute(connection));
			Assertions.assertEquals(0, connection.getCache().getHits());
			Assertions.assertEquals(1, connection.getCache().getMisses());
			ResultSet resultSet = null;
			int count = 100;
			for (int index = 0; index < count; index++) {
				resultSet = executor.execute(connection);
			}
			Assertions.assertEquals(count, connection.getCache().getHits());
			Assertions.assertEquals(1, connection.getCache().getMisses());
			assertEquals(executor.execute(databaseConnection), resultSet);
		}
	}

	protected void testResultSetMetaData(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws SQLException {
		try (SidecarConnection connection = connection(databaseContainer, redis)) {
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
