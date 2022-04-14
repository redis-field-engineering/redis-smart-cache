package com.redis.sidecar;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.redis.sidecar.impl.RedisResultSetCache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	static final String JDBC_URL_PREFIX = "jdbc:sidecar:";
	public static final String DRIVER_URL = "sidecar.driver.url";
	public static final String DRIVER_CLASS = "sidecar.driver.class";

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
		String driverClass = info.getProperty(DRIVER_CLASS);
		if (isEmpty(driverClass)) {
			throw new SQLException("No backend driver class specified");
		}
		Driver driver;
		try {
			driver = (Driver) Class.forName(driverClass).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + driverClass + "'", e);
		}
		String driverUrl = info.getProperty(DRIVER_URL);
		if (isEmpty(driverUrl)) {
			throw new SQLException("No backend URL specified");
		}
		Connection connection = driver.connect(driverUrl, info);
		String redisURI = url.substring(JDBC_URL_PREFIX.length());
		RedisResultSetCache resultSetCache = cache(redisURI);
		return new SidecarConnection(connection, resultSetCache);
	}

	private RedisResultSetCache cache(String redisURI) {
		RedisClient client = isEmpty(redisURI) ? RedisClient.create() : RedisClient.create(redisURI);
		return new RedisResultSetCache(client.connect(codec()));
	}

	public static RedisCodec<String, byte[]> codec() {
		return RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);
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
		return new DriverPropertyInfo[] { new DriverPropertyInfo(DRIVER_URL, null),
				new DriverPropertyInfo(DRIVER_CLASS, null) };
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
