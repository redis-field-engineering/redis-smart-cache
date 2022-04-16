package com.redis.sidecar;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.sidecar.impl.RedisResultSetCache;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

abstract class AbstractSidecarTests extends AbstractTestcontainersRedisTestBase {

	private final RedisContainer redis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
//	private final RedisClusterContainer redisCluster = new RedisClusterContainer(
//			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
//					.withKeyspaceNotifications();

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

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException {
		Class.forName(SidecarDriver.class.getName());
		Driver driver = DriverManager.getDriver(SidecarDriver.JDBC_URL_PREFIX + redis.getRedisURI());
		Assert.assertNotNull(driver);
		Assert.assertTrue(driver.getMajorVersion() >= 0);
		Assert.assertTrue(driver.getMinorVersion() >= 0);
		Assert.assertNotNull(driver.getParentLogger());
		Assert.assertFalse(driver.jdbcCompliant());
		DriverPropertyInfo[] infos = driver.getPropertyInfo(null, null);
		Assert.assertNotNull(infos);
		Assert.assertEquals(2, infos.length);
		Assert.assertEquals(SidecarDriver.DRIVER_URL, infos[0].name);
		Assert.assertEquals(SidecarDriver.DRIVER_CLASS, infos[1].name);
	}

	protected Connection getDatabaseConnection(JdbcDatabaseContainer<?> container) throws SQLException {
		try {
			Class.forName(container.getDriverClassName());
		} catch (ClassNotFoundException e) {
			throw new SQLException("Could not initialize driver", e);
		}
		return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
	}

	protected SidecarConnection getSidecarConnection(JdbcDatabaseContainer<?> database, RedisServer redis)
			throws SQLException {
		Connection connection = getDatabaseConnection(database);
		return new SidecarConnection(connection, new RedisResultSetCache(redis.getRedisURI()));
	}

	protected void assertEquals(ResultSet expected, ResultSet actual) throws SQLException {
		ResultSetMetaData meta1 = expected.getMetaData();
		ResultSetMetaData meta2 = actual.getMetaData();
		Assertions.assertEquals(meta1.getColumnCount(), meta2.getColumnCount());
		for (int index = 0; index < meta1.getColumnCount(); index++) {
			Assertions.assertEquals(meta1.getColumnName(index + 1), meta2.getColumnName(index + 1));
			Assertions.assertEquals(meta1.getColumnType(index + 1), meta2.getColumnType(index + 1));
		}
		int count = 0;
		while (expected.next()) {
			Assertions.assertTrue(actual.next());
			for (int index = 1; index <= meta1.getColumnCount(); index++) {
				Object expectedValue = expected.getObject(index);
				Object actualValue = actual.getObject(index);
				if (expectedValue instanceof Number) {
					expectedValue = ((Number) expectedValue).doubleValue();
					actualValue = ((Number) actualValue).doubleValue();
				}
				Assertions.assertEquals(expectedValue, actualValue,
						String.format("Column %s type %s", meta1.getColumnName(index), meta1.getColumnType(index)));
			}
			if (expected.getType() != ResultSet.TYPE_FORWARD_ONLY && actual.getType() != ResultSet.TYPE_FORWARD_ONLY) {
				Assertions.assertEquals(expected.isLast(), actual.isLast());
			}
			count++;
		}
		Assertions.assertTrue(count > 0);
	}

	private static interface StatementExecutor {

		ResultSet execute(Connection connection) throws SQLException;

	}

	protected void testSimpleStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql)
			throws SQLException {
		test(databaseContainer, redis.getServer(), c -> c.createStatement().executeQuery(sql));
	}

	protected void testUpdateAndGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql) throws SQLException {
		test(databaseContainer, redis.getServer(), c -> {
			Statement statement = c.createStatement();
			statement.execute(sql);
			return statement.getResultSet();
		});
	}

	protected void testPreparedStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws SQLException {
		test(databaseContainer, redis.getServer(), c -> {
			PreparedStatement statement = c.prepareStatement(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatement(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis, String sql,
			Object... parameters) throws SQLException {
		test(databaseContainer, redis.getServer(), c -> {
			CallableStatement statement = c.prepareCall(sql);
			for (int index = 0; index < parameters.length; index++) {
				statement.setObject(index + 1, parameters[index]);
			}
			return statement.executeQuery();
		});
	}

	protected void testCallableStatementGetResultSet(JdbcDatabaseContainer<?> databaseContainer, RedisTestContext redis,
			String sql, Object... parameters) throws SQLException {
		test(databaseContainer, redis.getServer(), c -> {
			CallableStatement statement = c.prepareCall(sql);
			statement.execute();
			return statement.getResultSet();
		});
	}

	private <T extends Statement> void test(JdbcDatabaseContainer<?> databaseContainer, RedisServer redis,
			StatementExecutor executor) throws SQLException {
		try (Connection databaseConnection = getDatabaseConnection(databaseContainer);
				SidecarConnection connection = getSidecarConnection(databaseContainer, redis)) {
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

	protected void testResultSetMetaData(JdbcDatabaseContainer<?> databaseContainer, RedisServer redis, String sql)
			throws SQLException {
		try (SidecarConnection connection = getSidecarConnection(databaseContainer, redis)) {
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
				Assert.assertFalse(metaData.isReadOnly(i));
				Assert.assertNotNull(metaData.getSchemaName(i));
				Assert.assertNotNull(metaData.getTableName(i));
				Assert.assertTrue(metaData.getPrecision(i) > 0);
				Assert.assertTrue(metaData.isSearchable(i));
				if (i == 1) {
					Assert.assertTrue(metaData.isSigned(i));
					Assert.assertEquals(0, metaData.getScale(i));
					Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(i));
					Assert.assertFalse(metaData.isCaseSensitive(i));
					Assert.assertFalse(metaData.isAutoIncrement(i));
					Assert.assertFalse(metaData.isCurrency(i));
//					Assert.assertFalse(metaData.isWritable(i));
				} else {
					Assert.assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(i));
				}
			}
		}
	}

}
