package com.redis.sidecar;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.sql.rowset.CachedRowSet;

import com.redis.sidecar.Config.Rule;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Table;

public class SidecarStatement implements Statement {

	private static final Logger log = Logger.getLogger(SidecarStatement.class.getName());
	private static final String QUERY_TIMER_ID = "queries";
	private static final String PARAMETER_SEPARATOR = ":";

	private final SqlParser parser = new SqlParser();
	private final SidecarConnection connection;
	private final Statement statement;
	private final Timer queryTimer;

	private String sql;
	private long ttl = Rule.TTL_NO_CACHE;
	private Optional<ResultSet> resultSet = Optional.empty();

	public SidecarStatement(SidecarConnection connection, Statement statement, MeterRegistry meterRegistry) {
		this.connection = connection;
		this.statement = statement;
		this.queryTimer = meterRegistry.timer(QUERY_TIMER_ID);
	}

	protected SidecarStatement(SidecarConnection connection, Statement statement, MeterRegistry meterRegistry,
			String sql) {
		this(connection, statement, meterRegistry);
		this.sql = sql;
	}

	protected final StringBuilder appendParameter(StringBuilder stringBuilder, String parameter) {
		return stringBuilder.append(PARAMETER_SEPARATOR).append(parameter);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return statement.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return statement.isWrapperFor(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql, () -> statement.executeQuery(sql));
	}

	protected boolean execute(String sql, Executable executable) throws SQLException {
		this.sql = sql;
		return execute(executable);
	}

	protected boolean execute(Executable executable) throws SQLException {
		try {
			return queryTimer.recordCallable(() -> doExecute(executable));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			// Should not happen but rethrow anyway
			throw new SQLException(e);
		}
	}

	private boolean doExecute(Executable executable) throws SQLException {
		checkClosed();
		if (getCachedResultSet().isPresent()) {
			return true;
		}
		return executable.execute();
	}

	private void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new SQLException("This statement has been closed.");
		}
	}

	protected ResultSet executeQuery(String sql, QueryExecutable executable) throws SQLException {
		this.sql = sql;
		return executeQuery(executable);
	}

	protected ResultSet executeQuery(QueryExecutable executable) throws SQLException {
		try {
			return queryTimer.recordCallable(() -> doExecuteQuery(executable));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			// Should not happen but rethrow anyway
			throw new SQLException(e);
		}
	}

	private ResultSet doExecuteQuery(QueryExecutable executable) throws SQLException {
		checkClosed();
		Optional<ResultSet> cachedResultSet = getCachedResultSet();
		if (cachedResultSet.isPresent()) {
			return cachedResultSet.get();
		}
		return cache(executable.execute());
	}

	private ResultSet cache(ResultSet resultSet) throws SQLException {
		if (resultSet == null || !isCachingEnabled()) {
			return resultSet;
		}
		CachedRowSet rowSet = connection.createCachedRowSet();
		rowSet.populate(resultSet);
		connection.getCache().put(key(), ttl, rowSet);
		rowSet.beforeFirst();
		return rowSet;
	}

	private boolean isCachingEnabled() {
		return ttl != Rule.TTL_NO_CACHE;
	}

	protected String key() {
		return sql;
	}

	private Optional<ResultSet> getCachedResultSet() {
		String key = key();
		List<String> tables;
		try {
			tables = parser.getTables(sql).map(Table::getName).map(QualifiedName::toString)
					.collect(Collectors.toList());
		} catch (ParsingException e) {
			log.log(Level.FINE, String.format("Could not parse SQL: %s", sql), e);
			return Optional.empty();
		}
		if (tables.isEmpty()) {
			return Optional.empty();
		}
		for (Rule rule : connection.getConfig().getRules()) {
			if (rule.getTable() == null || rule.getTable().isEmpty() || tables.contains(rule.getTable())) {
				ttl = rule.getTtl();
			}
		}
		if (isCachingEnabled()) {
			resultSet = connection.getCache().get(key);
			return resultSet;
		}
		return Optional.empty();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return statement.executeUpdate(sql);
	}

	@Override
	public void close() throws SQLException {
		statement.close();
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

	@Override
	public boolean execute(String sql) throws SQLException {
		return execute(sql, () -> statement.execute(sql));
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		if (resultSet.isPresent()) {
			return resultSet.get();
		}
		return cache(statement.getResultSet());
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
	public Connection getConnection() throws SQLException {
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
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return execute(sql, () -> statement.execute(sql, autoGeneratedKeys));
	}

	protected static interface Executable {

		boolean execute() throws SQLException;
	}

	protected static interface QueryExecutable {

		ResultSet execute() throws SQLException;

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

}
