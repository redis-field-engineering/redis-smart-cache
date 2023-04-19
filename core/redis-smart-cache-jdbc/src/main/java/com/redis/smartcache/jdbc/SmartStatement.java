package com.redis.smartcache.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.sql.RowSet;

import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryAction;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.RequiredSearch;

public class SmartStatement implements Statement {

	public static final String METER_QUERY = "query";
	public static final String METER_BACKEND = "backend";
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

	protected final SmartConnection connection;
	protected final Statement statement;
	private QueryAction action;
	private Optional<RowSet> rowSet = Optional.empty();

	public SmartStatement(SmartConnection connection, Statement statement) {
		this.connection = connection;
		this.statement = statement;
	}

	private RequiredSearch getMeter(String name, Query query, Tag... tags) {
		return connection.getMeterRegistry().get(name).tags(tags(query).and(tags));
	}

	private Timer getTimer(String name, Query query, Tag... tags) {
		return getMeter(name, query, tags).timer();
	}

	private Timer createTimer(String name, Query query) {
		return Timer.builder(name).tags(tags(query)).publishPercentiles(0.9, 0.99)
				.register(connection.getMeterRegistry());
	}

	private Tags tags(Query query) {
		return Tags.of(TAG_ID, query.getId(), TAG_TYPE, statementType(), TAG_SQL, query.getSql(), TAG_TABLE,
				csv(query.getTables()));
	}

	protected String statementType() {
		return TYPE;
	}

	private String csv(Collection<String> tables) {
		return tables.stream().collect(Collectors.joining(","));
	}

	private Counter createCounter(String name, Query query, String... tags) {
		return Counter.builder(name).tags(tags(query)).tags(tags).register(connection.getMeterRegistry());
	}

	private QueryAction queryAction(String sql) {
		Query query = getQuery(sql);
		String key = key(query);
		return new QueryAction(key, query, connection.getRuleSession().fire(query));
	}

	private Query getQuery(String sql) {
		return connection.computeQueryIfAbsent(sql, this::newQuery);
	}

	private Query newQuery(String sql) {
		Query query = new Query();
		query.setId(connection.hash(sql));
		query.setSql(sql);
		query.setTables(connection.tableNames(sql));
		createMeters(query);
		return query;
	}

	private void createMeters(Query query) {
		createTimer(METER_QUERY, query);
		createTimer(METER_BACKEND, query);
		createTimer(METER_CACHE_GET, query);
		createTimer(METER_CACHE_PUT, query);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_HIT);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_MISS);
	}

	protected String key(Query query) {
		return connection.getKeyBuilder().build(query.getId());
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
		return executeQuery(sql, () -> statement.executeQuery(sql));
	}

	protected ResultSet executeQuery(String sql, Callable<ResultSet> callable) throws SQLException {
		QueryAction executeAction = queryAction(sql);
		return execute(executeAction, () -> {
			Optional<RowSet> cached = getRowSet(executeAction);
			if (cached.isPresent()) {
				return cached.get();
			}
			Timer backendTimer = getTimer(METER_BACKEND, executeAction.getQuery());
			try {
				ResultSet backendResultSet = backendTimer.recordCallable(callable);
				if (executeAction.isCaching()) {
					return put(executeAction, backendResultSet);
				}
				return backendResultSet;
			} catch (SQLException e) {
				throw e;
			} catch (Exception e) {
				throw new SQLException(e);
			}
		});
	}

	private Optional<RowSet> getRowSet(QueryAction action) {
		if (!action.isCaching()) {
			return Optional.empty();
		}
		RequiredSearch meter = getMeter(METER_CACHE_GET, action.getQuery());
		Optional<RowSet> cached = Optional.ofNullable(meter.timer().record(() -> doGetRowSet(action)));
		meter.tag(TAG_RESULT, cached.isPresent() ? TAG_HIT : TAG_MISS).counter().increment();
		return cached;
	}

	private RowSet doGetRowSet(QueryAction action) {
		return connection.getRowSetCache().get(action.getKey());
	}

	private RowSet put(QueryAction action, ResultSet resultSet) throws SQLException {
		Timer cachePutTimer = getTimer(METER_CACHE_PUT, action.getQuery());
		try {
			return cachePutTimer.recordCallable(() -> doPut(action, resultSet));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private RowSet doPut(QueryAction action, ResultSet resultSet) throws SQLException {
		return connection.getRowSetCache().put(action.getKey(), action.getAction().getTtl(), resultSet);
	}

	private void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new SQLException("This statement has been closed.");
		}
	}

	protected boolean execute(String sql, Callable<Boolean> executable) throws SQLException {
		this.action = queryAction(sql);
		return execute(action, () -> {
			this.rowSet = getRowSet(action);
			if (rowSet.isPresent()) {
				return true;
			}
			return executable.call();
		});
	}

	private <T> T execute(QueryAction action, Callable<T> callable) throws SQLException {
		try {
			return getTimer(METER_QUERY, action.getQuery()).recordCallable(callable);
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
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
		if (rowSet.isPresent()) {
			return rowSet.get();
		}
		ResultSet resultSet = statement.getResultSet();
		if (action.isCaching()) {
			return put(action, resultSet);
		}
		return resultSet;
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
		action = null;
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
