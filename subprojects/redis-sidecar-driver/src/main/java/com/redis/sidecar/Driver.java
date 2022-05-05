package com.redis.sidecar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetProvider;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.core.ByteArrayResultSetCodec;
import com.redis.sidecar.core.Config;
import com.redis.sidecar.core.RedisStringCache;
import com.redis.sidecar.core.ResultSetCache;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class Driver implements java.sql.Driver {

	private static final Logger log = Logger.getLogger(Driver.class.getName());

	public static final String JDBC_URL_REGEX = "jdbc\\:(rediss?\\:\\/\\/.*\\w+.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);

	static {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		Config config = Config.load(info);
		config.setRedisURI(matcher.group(1));
		if (isEmpty(config.getDriverClass())) {
			throw new SQLException("No backend driver class specified");
		}
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(config.getDriverClass()).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + config.getDriverClass() + "'", e);
		}
		if (isEmpty(config.getDriverURL())) {
			throw new SQLException("No backend URL specified");
		}
		Connection connection = driver.connect(config.getDriverURL(), info);
		return new SidecarConnection(connection, cache(config), RowSetProvider.newFactory());
	}

	public static ResultSetCache cache(Config config) {
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
		if (url == null) {
			return false;
		}
		return JDBC_URL_PATTERN.matcher(url).find();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		return new DriverPropertyInfo[] {
				new DriverPropertyInfo(Config.PROPERTY_DRIVER_URL,
						info.getProperty(Config.PROPERTY_DRIVER_URL)),
				new DriverPropertyInfo(Config.PROPERTY_DRIVER_CLASS,
						info.getProperty(Config.PROPERTY_DRIVER_CLASS)) };
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
