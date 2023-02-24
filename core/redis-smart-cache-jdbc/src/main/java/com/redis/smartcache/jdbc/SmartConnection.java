package com.redis.smartcache.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;

import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.core.ResultSetCache;
import com.redis.smartcache.core.util.CRC32HashingFunction;
import com.redis.smartcache.core.util.EvictingLinkedHashMap;
import com.redis.smartcache.core.util.HashingFunction;
import com.redis.smartcache.core.util.SQLParser;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class SmartConnection implements Connection {

	private static final String METER_PREFIX_CACHE = "cache";
	private static final String METER_QUERY = "query";
	private static final String METER_BACKEND = "backend";
	private static final String METER_CACHE_GET = METER_PREFIX_CACHE + ".get";
	private static final String METER_CACHE_PUT = METER_PREFIX_CACHE + ".put";
	private static final String TAG_RESULT = "result";
	private static final String TAG_MISS = "miss";
	private static final String TAG_HIT = "hit";
	private static final String TAG_QUERY = "query";
	private final Map<String, Query> queryCache;

	private final HashingFunction hashFunction = new CRC32HashingFunction();
	private final SQLParser sqlParser = new SQLParser();
	private final Connection connection;
	private final QueryRuleSession ruleSession;
	private final RowSetFactory rowSetFactory;
	private final ResultSetCache resultSetCache;
	private final MeterRegistry meterRegistry;
	private final Config config;

	public SmartConnection(Connection connection, QueryRuleSession ruleSession, RowSetFactory rowSetFactory,
			ResultSetCache resultSetCache, MeterRegistry meterRegistry, Config config) {
		this.connection = connection;
		this.ruleSession = ruleSession;
		this.rowSetFactory = rowSetFactory;
		this.resultSetCache = resultSetCache;
		this.meterRegistry = meterRegistry;
		this.config = config;
		this.queryCache = new EvictingLinkedHashMap<>(config.getQueryCacheCapacity());
	}

	public Config getConfig() {
		return config;
	}

	public ResultSetCache getResultSetCache() {
		return resultSetCache;
	}

	@Override
	public void close() throws SQLException {
		connection.close();
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
		return new SmartStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql);
		return new SmartPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		CallableStatement statement = connection.prepareCall(sql);
		return new SmartCallableStatement(this, statement, sql);
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
	public Statement createStatement(int rsType, int rsConcurrency) throws SQLException {
		Statement statement = connection.createStatement(rsType, rsConcurrency);
		return new SmartStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int rsType, int rsConcurrency) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, rsType, rsConcurrency);
		return new SmartPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int rsType, int rsConcurrency) throws SQLException {
		CallableStatement statement = connection.prepareCall(sql, rsType, rsConcurrency);
		return new SmartCallableStatement(this, statement, sql);
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
	public Statement createStatement(int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
		Statement statement = connection.createStatement(rsType, rsConcurrency, rsHoldability);
		return new SmartStatement(this, statement);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int rsType, int rsConcurrency, int rsHoldability)
			throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, rsType, rsConcurrency, rsHoldability);
		return new SmartPreparedStatement(this, statement, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int rsType, int rsConcurrency, int rsHoldability)
			throws SQLException {
		CallableStatement statement = connection.prepareCall(sql, rsType, rsConcurrency, rsHoldability);
		return new SmartCallableStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, autoGeneratedKeys);
		return new SmartPreparedStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, columnIndexes);
		return new SmartPreparedStatement(this, statement, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql, columnNames);
		return new SmartPreparedStatement(this, statement, sql);
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

	public String hash(String string) {
		return hashFunction.hash(string);
	}

	public Query getQuery(String sql) {
		String id = hash(sql);
		if (queryCache.containsKey(id)) {
			return queryCache.get(id);
		}
		Set<String> tables = sqlParser.extractTableNames(sql);
		Query query = new Query(id, sql, tables);
		createTimer(METER_QUERY, query);
		createTimer(METER_BACKEND, query);
		createTimer(METER_CACHE_GET, query);
		createTimer(METER_CACHE_PUT, query);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_HIT);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_MISS);
		return query;
	}

	public Timer getQueryTimer(Query query) {
		return getTimer(METER_QUERY, query);
	}

	public Timer getBackendTimer(Query query) {
		return getTimer(METER_BACKEND, query);
	}

	public Timer getCacheGetTimer(Query query) {
		return getTimer(METER_CACHE_GET, query);
	}

	public Timer getCachePutTimer(Query query) {
		return getTimer(METER_CACHE_PUT, query);
	}

	public Counter getCacheHitCounter(Query query) {
		return getCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_HIT);
	}

	public Counter getCacheMissCounter(Query query) {
		return getCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_MISS);
	}

	private Counter getCounter(String name, Query query, String... tags) {
		return meterRegistry.get(name).tags(tags(query)).tags(tags).counter();
	}

	private Timer getTimer(String name, Query query) {
		return meterRegistry.get(name).tags(tags(query)).timer();
	}

	private Timer createTimer(String name, Query query) {
		return Timer.builder(name).tags(tags(query)).publishPercentiles(0.9, 0.99).register(meterRegistry);
	}

	private Tags tags(Query query) {
		return Tags.of(TAG_QUERY, query.getId());
	}

	private Counter createCounter(String name, Query query, String... tags) {
		return Counter.builder(name).tags(tags(query)).tags(tags).register(meterRegistry);
	}

	public Query fireRules(String sql) {
		Query query = getQuery(sql);
		ruleSession.fire(query);
		return query;
	}

	public CachedRowSet createCachedRowSet() throws SQLException {
		return rowSetFactory.createCachedRowSet();
	}

}
