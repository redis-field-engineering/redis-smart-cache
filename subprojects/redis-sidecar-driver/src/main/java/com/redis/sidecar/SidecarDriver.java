package com.redis.sidecar;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.impl.RedisClusterResultSetCache;
import com.redis.sidecar.impl.RedisResultSetCache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	static final String JDBC_URL_PREFIX = "jdbc:sidecar:";
	public static final String PROPERTY_DRIVER_URL = "sidecar.driver.url";
	public static final String PROPERTY_DRIVER_CLASS = "sidecar.driver.class";
	public static final String PROPERTY_REDIS_CLUSTER = "sidecar.redis.cluster";
	public static final String PROPERTY_REDIS_POOL_SIZE = "sidecar.redis.pool";

	static {
		try {
			DriverManager.registerDriver(new SidecarDriver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		String driverClass = info.getProperty(PROPERTY_DRIVER_CLASS);
		if (isEmpty(driverClass)) {
			throw new SQLException("No backend driver class specified");
		}
		Driver driver;
		try {
			driver = (Driver) Class.forName(driverClass).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + driverClass + "'", e);
		}
		String driverUrl = info.getProperty(PROPERTY_DRIVER_URL);
		if (isEmpty(driverUrl)) {
			throw new SQLException("No backend URL specified");
		}
		Connection connection = driver.connect(driverUrl, info);
		String redisURI = url.substring(JDBC_URL_PREFIX.length());
		boolean cluster = info.getProperty(PROPERTY_REDIS_CLUSTER, "false").equalsIgnoreCase("true");
		int poolSize = Integer.parseInt(
				info.getProperty(PROPERTY_REDIS_POOL_SIZE, String.valueOf(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL)));
		return new SidecarConnection(connection, resultSetCache(redisURI, cluster, poolSize));
	}

	private ResultSetCache resultSetCache(String redisURI, boolean cluster, int poolMax) {
		GenericObjectPoolConfig<StatefulConnection<String, byte[]>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(poolMax);
		if (cluster) {
			return new RedisClusterResultSetCache(RedisClusterClient.create(redisURI), poolConfig);
		}
		return new RedisResultSetCache(RedisClient.create(redisURI), poolConfig);

	}

	private boolean isEmpty(String string) {
		return string == null || string.isEmpty();
	}

	@Override
	public boolean acceptsURL(String url) {
		return !isEmpty(url) && url.startsWith(JDBC_URL_PREFIX) && url.length() > JDBC_URL_PREFIX.length();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		return new DriverPropertyInfo[] { new DriverPropertyInfo(PROPERTY_DRIVER_URL, null),
				new DriverPropertyInfo(PROPERTY_DRIVER_CLASS, null) };
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() {
		return log;
	}

}
