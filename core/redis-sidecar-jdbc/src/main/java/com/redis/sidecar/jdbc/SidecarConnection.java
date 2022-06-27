package com.redis.sidecar.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.core.ByteArrayResultSetCodec;
import com.redis.sidecar.core.Config;
import com.redis.sidecar.core.Config.Redis.Pool;
import com.redis.sidecar.core.ResultSetCache;
import com.redis.sidecar.core.StringResultSetCache;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.MeterRegistry;

public class SidecarConnection implements Connection {

	private final Connection connection;
	private final Config config;
	private final ResultSetCache cache;
	private final RowSetFactory rowSetFactory;
	private final MeterRegistry meterRegistry;

	public SidecarConnection(Connection connection, AbstractRedisClient redisClient, Config config,
			MeterRegistry meterRegistry) throws SQLException {
		LettuceAssert.notNull(connection, "Connection is required");
		LettuceAssert.notNull(redisClient, "Redis client is required");
		LettuceAssert.notNull(config, "Config is required");
		this.connection = connection;
		this.config = config;
		this.meterRegistry = meterRegistry;
		this.rowSetFactory = RowSetProvider.newFactory();
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(rowSetFactory, config.getBufferSize(),
				meterRegistry);
		Supplier<StatefulConnection<String, ResultSet>> connectionSupplier = redisClient instanceof RedisClusterClient
				? () -> ((RedisClusterClient) redisClient).connect(codec)
				: () -> ((RedisClient) redisClient).connect(codec);
		this.cache = new StringResultSetCache(meterRegistry,
				ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig(config)),
				sync(redisClient));
	}

	private static <T> GenericObjectPoolConfig<T> poolConfig(Config config) {
		Pool pool = config.getRedis().getPool();
		GenericObjectPoolConfig<T> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(pool.getMaxActive());
		poolConfig.setMaxIdle(pool.getMaxIdle());
		poolConfig.setMinIdle(pool.getMinIdle());
		poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		poolConfig.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return poolConfig;
	}

	private Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync(
			AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<String, ResultSet>) c).sync();
		}
		return c -> ((StatefulRedisConnection<String, ResultSet>) c).sync();
	}

	public Config getConfig() {
		return config;
	}

	@Override
	public void close() throws SQLException {
		connection.close();
		try {
			cache.close();
		} catch (Exception e) {
			throw new SQLException("Could not close ResultSetCache", e);
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return connection.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return connection.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		Statement statement = connection.createStatement();
		return new SidecarStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		CallableStatement statement = connection.prepareCall(sql);
		return new SidecarCallableStatement(this, statement, sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return connection.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		connection.commit();
	}

	@Override
	public void rollback() throws SQLException {
		connection.rollback();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		connection.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return connection.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		connection.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return connection.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		connection.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return connection.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return connection.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		connection.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency);
		return new SidecarStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
		return new SidecarCallableStatement(this, statement, sql);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return connection.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		connection.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		connection.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return connection.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return connection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return connection.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		connection.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		return new SidecarStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
		return new SidecarCallableStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, autoGeneratedKeys);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, columnIndexes);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, columnNames);
		return new SidecarPreparedStatement(this, statement, sql);
	}

	@Override
	public Clob createClob() throws SQLException {
		return connection.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return connection.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return connection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return connection.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return connection.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		connection.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		connection.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return connection.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return connection.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return connection.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return connection.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		connection.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return connection.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		connection.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		connection.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return connection.getNetworkTimeout();
	}

	public ResultSetCache getCache() {
		return cache;
	}

	public CachedRowSet createCachedRowSet() throws SQLException {
		return rowSetFactory.createCachedRowSet();
	}

	public MeterRegistry getMeterRegistry() {
		return meterRegistry;
	}

}
