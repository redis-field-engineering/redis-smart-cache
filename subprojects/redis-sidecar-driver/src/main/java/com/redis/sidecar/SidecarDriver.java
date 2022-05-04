package com.redis.sidecar;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.rowset.RowSetProvider;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.impl.ByteArrayResultSetCodec;
import com.redis.sidecar.impl.RedisStringCache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	public static final String JDBC_URL_PREFIX = "jdbc:sidecar:";

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
		SidecarConfig config = SidecarConfig.load(info);
		if (isEmpty(config.getDriverClass())) {
			throw new SQLException("No backend driver class specified");
		}
		Driver driver;
		try {
			driver = (Driver) Class.forName(config.getDriverClass()).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + config.getDriverClass() + "'", e);
		}
		if (isEmpty(config.getDriverURL())) {
			throw new SQLException("No backend URL specified");
		}
		Connection connection = driver.connect(config.getDriverURL(), info);
		config.setRedisURI(url.substring(JDBC_URL_PREFIX.length()));
		return new SidecarConnection(connection, cache(config), RowSetProvider.newFactory());
	}

	public static ResultSetCache cache(SidecarConfig config) {
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(config.getByteBufferSize());
		GenericObjectPoolConfig<StatefulConnection<String, ResultSet>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.getPoolSize());
		if (config.isRedisCluster()) {
			RedisClusterClient client = RedisClusterClient.create(config.getRedisURI());
			return new RedisStringCache(() -> client.connect(codec), poolConfig,
					c -> ((StatefulRedisClusterConnection<String, ResultSet>) c).sync());
		}
		RedisClient client = RedisClient.create(config.getRedisURI());
		return new RedisStringCache(() -> client.connect(codec), poolConfig,
				c -> ((StatefulRedisConnection<String, ResultSet>) c).sync());

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
		return new DriverPropertyInfo[] {
				new DriverPropertyInfo(SidecarConfig.PROPERTY_DRIVER_URL,
						info.getProperty(SidecarConfig.PROPERTY_DRIVER_URL)),
				new DriverPropertyInfo(SidecarConfig.PROPERTY_DRIVER_CLASS,
						info.getProperty(SidecarConfig.PROPERTY_DRIVER_CLASS)) };
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
