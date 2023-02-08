package com.redis.sidecar;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NonRegisteringDriver implements Driver {

	private static final Logger log = Logger.getLogger(NonRegisteringDriver.class.getName());

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final String PROPERTY_DRIVER_PREFIX = PropsMapper.PROPERTY_PREFIX + ".driver";

	private static final Map<String, Driver> drivers = new HashMap<>();
	private static final Map<ContextId, ConnectionContext> contexts = new HashMap<>();

	private final PropsMapper propsMapper = new PropsMapper();

	public NonRegisteringDriver() {
		// Needed for Class.forName().newInstance()
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
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
		return new SidecarConnection(backendConnection, context);
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
		Driver driver;
		try {
			driver = driver(className);
		} catch (Exception e) {
			throw new SQLException("Could not load driver class '" + className + "'", e);
		}
		return driver.connect(url, info);
	}

	private synchronized Driver driver(String className)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (drivers.containsKey(className)) {
			return drivers.get(className);
		}
		Driver driver = (Driver) Class.forName(className).getConstructor().newInstance();
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
				new DriverPropertyInfo(PROPERTY_DRIVER_PREFIX + ".url",
						info.getProperty(PROPERTY_DRIVER_PREFIX + ".url")),
				new DriverPropertyInfo(PROPERTY_DRIVER_PREFIX + ".class-name",
						info.getProperty(PROPERTY_DRIVER_PREFIX + ".class-name")) };
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
