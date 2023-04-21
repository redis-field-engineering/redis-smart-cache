package com.redis.smartcache.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.redis.smartcache.core.Action;
import com.redis.smartcache.core.Query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.RequiredSearch;

public class SmartStatement implements Statement {

	public static final String METER_QUERY = "query";
	public static final String METER_BACKEND = "backend";
	public static final String METER_BACKEND_RESULTSET = METER_BACKEND + ".resultset";
	public static final String METER_PREFIX_CACHE = "cache";
	public static final String METER_CACHE_GET = METER_PREFIX_CACHE + ".get";
	public static final String METER_CACHE_PUT = METER_PREFIX_CACHE + ".put";
	public static final String TAG_RESULT = "result";
	public static final String TAG_MISS = "miss";
	public static final String TAG_HIT = "hit";
	public static final String TAG_ID = "id";
	public static final String TAG_TABLE = "table";
	public static final String TAG_SQL = "sql";
	public static final String TAG_TYPE = "type";
	private static final String TYPE = "static";
	private static final double[] PERCENTILES = { 0.9, 0.99 };

	protected final SmartConnection connection;
	protected final Statement statement;
	private Query query;
	private Action action;
	private ResultSet resultSet;

	public SmartStatement(SmartConnection connection, Statement statement) {
		this.connection = connection;
		this.statement = statement;
	}

	protected void init(String sql) {
		this.query = connection.getQueryCache().computeIfAbsent(sql, this::newQuery);
		this.action = connection.getRuleSession().fire(query);
	}

	private boolean hasResultSet() {
		return resultSet != null;
	}

	private boolean isCaching() {
		return action != null && action.isCaching();
	}

	private Tags tags(Query query) {
		String tables = query.getTables().stream().collect(Collectors.joining(","));
		return Tags.of(TAG_ID, query.getId(), TAG_TYPE, statementType(), TAG_SQL, query.getSql(), TAG_TABLE, tables);
	}

	private RequiredSearch getMeter(String name) {
		return connection.getMeterRegistry().get(name).tags(tags(query));
	}

	private Timer createTimer(String name, Query query) {
		return Timer.builder(name).tags(tags(query)).publishPercentiles(PERCENTILES)
				.register(connection.getMeterRegistry());
	}

	private Counter createCounter(String name, Query query, String tagKey, String tagValue) {
		return Counter.builder(name).tags(tags(query)).tag(tagKey, tagValue).register(connection.getMeterRegistry());
	}

	protected String statementType() {
		return TYPE;
	}

	private Query newQuery(String sql) {
		Query newQuery = new Query();
		newQuery.setId(connection.hash(sql));
		newQuery.setSql(sql);
		newQuery.setTables(connection.tableNames(sql));
		createTimer(METER_QUERY, newQuery);
		createTimer(METER_BACKEND, newQuery);
		createTimer(METER_BACKEND_RESULTSET, newQuery);
		createTimer(METER_CACHE_GET, newQuery);
		createTimer(METER_CACHE_PUT, newQuery);
		createCounter(METER_CACHE_GET, newQuery, TAG_RESULT, TAG_HIT);
		createCounter(METER_CACHE_GET, newQuery, TAG_RESULT, TAG_MISS);
		return newQuery;
	}

	private String key() {
		return key(query.getId());
	}

	protected String key(String id) {
		return connection.getKeyBuilder().build(id);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		checkClosed();
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		checkClosed();
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		init(sql);
		return executeQuery(() -> statement.executeQuery(sql));
	}

	protected ResultSet executeQuery(Callable<ResultSet> callable) throws SQLException {
		return time(METER_QUERY, () -> {
			populateFromCache();
			return getResultSet(() -> executeBackend(callable));
		});
	}

	protected boolean execute(Callable<Boolean> callable) throws SQLException {
		return time(METER_QUERY, () -> {
			populateFromCache();
			if (hasResultSet()) {
				return true;
			}
			return executeBackend(callable);
		});
	}

	private ResultSet getResultSet(Callable<ResultSet> callable) throws SQLException {
		if (hasResultSet()) {
			return resultSet;
		}
		resultSet = time(METER_BACKEND_RESULTSET, callable);
		if (isCaching()) {
			resultSet = time(METER_CACHE_PUT, () -> connection.getRowSetCache().put(key(), action.getTtl(), resultSet));
		}
		return resultSet;
	}

	private <T> T executeBackend(Callable<T> callable) throws Exception {
		return getMeter(METER_BACKEND).timer().recordCallable(callable);
	}

	/**
	 * 
	 * @param query the query for which to get cached results
	 * @return cached result-set for the given query
	 * @throws SQLException
	 */
	private void populateFromCache() throws SQLException {
		if (!isCaching()) {
			return;
		}
		resultSet = time(METER_CACHE_GET, () -> connection.getRowSetCache().get(key()));
		getMeter(METER_CACHE_GET).tag(TAG_RESULT, hasResultSet() ? TAG_HIT : TAG_MISS).counter().increment();
	}

	private void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new SQLException("This statement has been closed.");
		}
	}

	private <T> T time(String meter, Callable<T> callable) throws SQLException {
		try {
			return getMeter(meter).timer().recordCallable(callable);
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private boolean execute(String sql, Callable<Boolean> callable) throws SQLException {
		init(sql);
		return execute(callable);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		return execute(sql, () -> statement.execute(sql));
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return execute(sql, () -> statement.execute(sql, autoGeneratedKeys));
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return execute(sql, () -> statement.execute(sql, columnIndexes));
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return execute(sql, () -> statement.execute(sql, columnNames));
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return getResultSet(statement::getResultSet);
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return statement.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return statement.getMoreResults();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		statement.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return statement.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		statement.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return statement.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return statement.getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return statement.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		statement.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		statement.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return statement.executeBatch();
	}

	@Override
	public SmartConnection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return statement.getMoreResults();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return statement.getGeneratedKeys();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return statement.executeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return statement.executeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return statement.executeUpdate(sql, columnNames);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return statement.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return statement.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		statement.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return statement.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		statement.closeOnCompletion();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return statement.isCloseOnCompletion();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return statement.executeUpdate(sql);
	}

	@Override
	public void close() throws SQLException {
		statement.close();
		query = null;
		action = null;
		resultSet = null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return statement.getMaxFieldSize();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		statement.setMaxFieldSize(max);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return statement.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		statement.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		statement.setEscapeProcessing(enable);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return statement.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		statement.setQueryTimeout(seconds);
	}

	@Override
	public void cancel() throws SQLException {
		statement.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return statement.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		statement.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		statement.setCursorName(name);
	}

}
