package com.redis.sidecar.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;

import javax.sql.rowset.CachedRowSet;

import com.redis.sidecar.core.Config;
import com.redis.sidecar.core.Config.Rule;

import io.micrometer.core.instrument.Timer;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SidecarStatement implements Statement {

	protected final SidecarConnection connection;
	private final Statement statement;
	private final Timer queryTimer;
	private final Timer databaseTimer;

	protected String sql;
	private long ttl;
	protected ResultSet resultSet;

	public SidecarStatement(SidecarConnection connection, Statement statement) {
		this.connection = connection;
		this.statement = statement;
		this.queryTimer = Timer.builder("query.latency").description("Query latency")
				.register(connection.getMeterRegistry());
		this.databaseTimer = Timer.builder("db.latency").description("Database query latency")
				.register(connection.getMeterRegistry());
	}

	protected SidecarStatement(SidecarConnection connection, Statement statement, String sql) throws SQLException {
		this(connection, statement);
		setSQL(sql);
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
		return recordQuery(() -> doExecuteQuery(sql));
	}

	protected <T> T recordQuery(Callable<T> callable) throws SQLException {
		return record(queryTimer, callable);
	}

	private ResultSet doExecuteQuery(String sql) throws SQLException {
		setSQL(sql);
		this.resultSet = get();
		if (this.resultSet == null) {
			ResultSet databaseResultSet = recordDatabase(() -> statement.executeQuery(sql));
			this.resultSet = cache(databaseResultSet);
		}
		return this.resultSet;
	}

	protected <T> T recordDatabase(Callable<T> callable) throws SQLException {
		return record(databaseTimer, callable);
	}

	private <T> T record(Timer timer, Callable<T> callable) throws SQLException {
		try {
			return timer.recordCallable(callable);
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	protected ResultSet get() throws SQLException {
		if (ttl == Config.TTL_NO_CACHE) {
			return null;
		}
		return connection.getCache().get(key(sql));
	}

	protected String key(String sql) {
		return sql;
	}

	protected ResultSet cache(ResultSet resultSet) throws SQLException {
		if (ttl == Config.TTL_NO_CACHE) {
			return resultSet;
		}
		CachedRowSet rowSet = connection.createCachedRowSet();
		rowSet.populate(resultSet);
		connection.getCache().put(key(sql), ttl, rowSet);
		rowSet.beforeFirst();
		return rowSet;
	}

	private void setSQL(String sql) {
		this.sql = sql;
		this.ttl = ttl(sql);
	}

	private long ttl(String sql) {
		net.sf.jsqlparser.statement.Statement statement;
		try {
			statement = CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e) {
			return Config.TTL_NO_CACHE;
		}
		Select selectStatement = (Select) statement;
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
		for (Rule rule : connection.getConfig().getRules()) {
			if (rule.getTable() == null || tableList.contains(rule.getTable())) {
				return rule.getTtl();
			}
		}
		return Config.TTL_NO_CACHE;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		setSQLNoCache(sql);
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
		return recordQuery(() -> doExecute(sql));
	}

	private boolean doExecute(String sql) throws SQLException {
		setSQL(sql);
		resultSet = get();
		if (resultSet == null) {
			recordDatabase(() -> statement.execute(sql));
		}
		return true;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		if (resultSet == null) {
			resultSet = cache(statement.getResultSet());
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
		setSQLNoCache(sql);
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
		setSQLNoCache(sql);
		return statement.executeUpdate(sql, autoGeneratedKeys);
	}

	private void setSQLNoCache(String sql) {
		this.sql = sql;
		this.ttl = Config.TTL_NO_CACHE;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		setSQLNoCache(sql);
		return statement.executeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		setSQLNoCache(sql);
		return statement.executeUpdate(sql, columnNames);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return recordQuery(() -> doExecute(sql, autoGeneratedKeys));
	}

	private boolean doExecute(String sql, int autoGeneratedKeys) throws SQLException {
		setSQL(sql);
		resultSet = get();
		if (resultSet == null) {
			return recordDatabase(() -> statement.execute(sql, autoGeneratedKeys));
		}
		return true;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return recordQuery(() -> doExecute(sql, columnIndexes));
	}

	private boolean doExecute(String sql, int[] columnIndexes) throws SQLException {
		setSQL(sql);
		resultSet = get();
		if (resultSet == null) {
			return recordDatabase(() -> statement.execute(sql, columnIndexes));
		}
		return true;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return recordQuery(() -> doExecute(sql, columnNames));
	}

	private boolean doExecute(String sql, String[] columnNames) throws SQLException {
		setSQL(sql);
		resultSet = get();
		if (resultSet == null) {
			return recordDatabase(() -> statement.execute(sql, columnNames));
		}
		return true;
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
