package com.redis.smartcache;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class Driver implements java.sql.Driver {

	private static final Logger log = Logger.getLogger(Driver.class.getName());

	public static final String PREFIX = "smartcache";
	public static final String PROPERTY_PREFIX_DRIVER = PREFIX + ".driver";
	public static final String PROPERTY_PREFIX_REDIS = PREFIX + ".redis";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final String CACHE_KEY_PREFIX = "cache";
	public static final Duration DEFAULT_CONFIG_STEP = Duration.ofSeconds(10);
	public static final Duration DEFAULT_METRICS_STEP = Duration.ofSeconds(60);
	public static final int DEFAULT_POOL_SIZE = 8;
	public static final DataSize DEFAULT_BYTE_BUFFER_CAPACITY = DataSize.of(10, Unit.MEGABYTE);

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);

	private static final Map<String, java.sql.Driver> drivers = new HashMap<>();
	private static final Map<ContextId, ConnectionContext> contexts = new HashMap<>();

	private final PropsMapper propsMapper = new PropsMapper();

	static {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (SQLException e) {
			throw new SQLRuntimeException("Can't register driver");
		}
	}

	public static class SQLRuntimeException extends RuntimeException {

		private static final long serialVersionUID = -6960977193373569598L;

		public SQLRuntimeException(String message) {
			super(message);
		}

	}

	public Driver() {
		// Needed for Class.forName().newInstance()
	}

	@Override
	public SmartCacheConnection connect(String url, Properties info) throws SQLException {
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.matches()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		String redisUri = matcher.group(1);
		BootstrapConfig bootstrap;
		try {
			bootstrap = propsMapper.read(info, BootstrapConfig.class);
		} catch (IOException e) {
			throw new SQLException("Could not load config", e);
		}
		bootstrap.getRedis().setUri(redisUri);
		Connection backendConnection = backendConnection(bootstrap, info);
		ContextId contextId = ContextId.of(redisUri, bootstrap.getRedis().getKeyspace());
		ConnectionContext context = contexts.computeIfAbsent(contextId, c -> new ConnectionContext(bootstrap));
		return new SmartCacheConnection(backendConnection, context);
	}

	private Connection backendConnection(BootstrapConfig config, Properties info) throws SQLException {
		String className = config.getDriver().getClassName();
		if (className == null || className.isEmpty()) {
			throw new SQLException("No backend driver class specified");
		}
		String url = config.getDriver().getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver;
		try {
			driver = driver(className);
		} catch (Exception e) {
			throw new SQLException("Could not load driver class '" + className + "'", e);
		}
		return driver.connect(url, info);
	}

	private synchronized java.sql.Driver driver(String className)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (drivers.containsKey(className)) {
			return drivers.get(className);
		}
		java.sql.Driver driver = (java.sql.Driver) Class.forName(className).getConstructor().newInstance();
		drivers.put(className, driver);
		return driver;
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
				new DriverPropertyInfo(PROPERTY_PREFIX_DRIVER + ".url",
						info.getProperty(PROPERTY_PREFIX_DRIVER + ".url")),
				new DriverPropertyInfo(PROPERTY_PREFIX_DRIVER + ".class-name",
						info.getProperty(PROPERTY_PREFIX_DRIVER + ".class-name")) };
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

	public void clear() {
		drivers.clear();
		contexts.forEach((k, v) -> v.clear());
	}

}
