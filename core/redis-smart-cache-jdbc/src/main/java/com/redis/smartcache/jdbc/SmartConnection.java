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
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

import com.redis.smartcache.Driver;
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryRuleSession;

import io.micrometer.core.instrument.MeterRegistry;

public class SmartConnection implements Connection {

    private static final Logger log = Logger.getLogger(SmartConnection.class.getName());

    private final UnaryOperator<String> hashFunction = Driver::crc32;

    private final SQLParser parser = new SQLParser();

    private final Connection connection;

    private final RowSetCache rowSetCache;

    private final MeterRegistry meterRegistry;

    private final QueryRuleSession session;

    private final KeyBuilder keyBuilder;

    private final Map<String, Query> queryCache;

    public SmartConnection(Connection connection, QueryRuleSession session, MeterRegistry meterRegistry,
            RowSetCache rowSetCache, Map<String, Query> queryCache, KeyBuilder keyBuilder) {
        this.connection = connection;
        this.session = session;
        this.meterRegistry = meterRegistry;
        this.rowSetCache = rowSetCache;
        this.queryCache = queryCache;
        this.keyBuilder = keyBuilder;
    }

    public KeyBuilder getKeyBuilder() {
        return keyBuilder;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public QueryRuleSession getRuleSession() {
        return session;
    }

    public RowSetCache getRowSetCache() {
        return rowSetCache;
    }

    public String hash(String string) {
        return hashFunction.apply(string);
    }

    @Override
    public void close() throws SQLException {
        log.fine("Closing backend connection");
        connection.close();
        log.fine("Closing RowSet cache");
        try {
            rowSetCache.close();
        } catch (Exception e) {
            throw new SQLException("Could not close RowSet cache", e);
        }
        log.fine("Closed connection");
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
    public CallableStatement prepareCall(String sql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
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

    public Set<String> tableNames(String sql) {
        return parser.extractTableNames(sql);
    }

    public Map<String, Query> getQueryCache() {
        return queryCache;
    }

}
