package com.redis.smartcache;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.RowSet;
import javax.sql.rowset.RowSetFactory;

import com.redis.smartcache.core.ClientManager;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.DriverConfig;
import com.redis.smartcache.core.EvictingLinkedHashMap;
import com.redis.smartcache.core.HashingFunctions;
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.Mappers;
import com.redis.smartcache.core.MeterRegistryManager;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.core.RuleSessionManager;
import com.redis.smartcache.jdbc.RedisResultSetCache;
import com.redis.smartcache.jdbc.ResultSetCache;
import com.redis.smartcache.jdbc.RowSetCodec;
import com.redis.smartcache.jdbc.SmartConnection;
import com.redis.smartcache.jdbc.rowset.CachedRowSetFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver
 * should supply a class that implements the Driver interface
 * 
 * The DriverManager will try to load as many drivers as it can find and then
 * for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 * 
 * It is strongly recommended that each Driver class should be small and
 * standalone so that the Driver class can be loaded and queried without
 * bringing in vast quantities of supporting code.
 * 
 * When a Driver class is loaded, it should create an instance of itself and
 * register it with the DriverManager. This means that a user can load and
 * register a driver by doing Class.forName("foo.bah.Driver")
 */
public class Driver implements java.sql.Driver {

	private static Driver registeredDriver;
	private static final Logger parentLog = Logger.getLogger("com.redis.smartcache");
	private static final Logger log = Logger.getLogger("com.redis.smartcache.Driver");

	public static final String KEYSPACE_QUERIES = "queries";
	public static final String KEYSPACE_CACHE = "cache";
	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final RowSetFactory ROW_SET_FACTORY = new CachedRowSetFactory();

	private static final ClientManager clientManager = new ClientManager();
	private static final RuleSessionManager ruleSessionManager = new RuleSessionManager(clientManager);
	private static final MeterRegistryManager registryManager = new MeterRegistryManager(clientManager);
	private static final Map<Config, Map<String, Query>> queryCaches = new HashMap<>();

	static {
		try {
			register();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	public SmartConnection connect(String url, Properties info) throws SQLException {
		// The driver should return "null" if it realizes it is the wrong kind of driver
		// to connect to the given URL.
		if (url == null) {
			throw new SQLException("URL is null");
		}
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.matches()) {
			log.log(Level.FINE, "URL {0} does not match pattern {1}", new Object[] { url, JDBC_URL_PATTERN.pattern() });
			return null;
		}
		String redisUri = matcher.group(1);
		if (redisUri == null || redisUri.isEmpty()) {
			log.fine("JDBC URL contains empty Redis URI");
			return null;
		}
		Config config;
		try {
			config = config(info);
		} catch (IOException e) {
			throw new SQLException("Could not load config", e);
		}
		config.getRedis().setUri(redisUri);
		log.fine("Creating backend connection");
		Connection backendConnection = backendConnection(config.getDriver(), info);
		log.fine("Creating SmartCache connection");
		return makeConnection(config, backendConnection);
	}

	public static Config config(Properties info) throws IOException {
		Properties properties = new Properties();
		try {
			properties.putAll(System.getenv());
			properties.putAll(System.getProperties());
		} catch (SecurityException e) {
			// Ignore since we don't require access to system environment
		}
		properties.putAll(info);
		return Mappers.config(properties);
	}

	private SmartConnection makeConnection(Config config, Connection backendConnection) {
		QueryRuleSession ruleSession = ruleSessionManager.getRuleSession(config);
		KeyBuilder keyBuilder = KeyBuilder.of(config).sub(KEYSPACE_CACHE);
		MeterRegistry registry = registryManager.getRegistry(config);
		Map<String, Query> queryCache = queryCaches.computeIfAbsent(config, this::createQueryCache);
		return new SmartConnection(backendConnection, ruleSession, registry, ROW_SET_FACTORY, rowSetCache(config),
				queryCache, keyBuilder);
	}

	private ResultSetCache rowSetCache(Config config) {
		AbstractRedisClient client = clientManager.getClient(config);
		RedisCodec<String, RowSet> codec = resultSetCodec(config);
		return new RedisResultSetCache(ROW_SET_FACTORY, client, codec);
	}

	private Map<String, Query> createQueryCache(Config config) {
		return Collections.synchronizedMap(new EvictingLinkedHashMap<>(config.getQueryCacheCapacity()));
	}

	private static RedisCodec<String, RowSet> resultSetCodec(Config config) {
		int bufferSize = Math.toIntExact(config.getRedis().getCodecBufferCapacity().toBytes());
		return new RowSetCodec(ROW_SET_FACTORY, bufferSize);
	}

	private Connection backendConnection(DriverConfig config, Properties info) throws SQLException {
		Properties backendInfo = new Properties();
		for (String name : info.stringPropertyNames()) {
			if (name.startsWith(Mappers.PROPERTY_PREFIX)) {
				continue;
			}
			backendInfo.setProperty(name, info.getProperty(name));
		}
		String url = config.getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver = backendDriver(config.getClassName());
		log.log(Level.FINE, "Connecting to backend database with URL: {0}", url);
		return driver.connect(url, backendInfo);
	}

	public synchronized java.sql.Driver backendDriver(String className) throws SQLException {
		if (className == null || className.isEmpty()) {
			throw new SQLException("No backend driver class specified");
		}
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(className).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Could not load backend driver class '" + className + "'", e);
		}
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
				new DriverPropertyInfo(Mappers.PROPERTY_PREFIX_DRIVER + ".url",
						info.getProperty(Mappers.PROPERTY_PREFIX_DRIVER + ".url")),
				new DriverPropertyInfo(Mappers.PROPERTY_PREFIX_DRIVER + ".class-name",
						info.getProperty(Mappers.PROPERTY_PREFIX_DRIVER + ".class-name")) };
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
		return parentLog;
	}

	public static void register() throws SQLException {
		if (isRegistered()) {
			throw new IllegalStateException("Driver is already registered. It can only be registered once.");
		}
		Driver driver = new Driver();
		DriverManager.registerDriver(driver);
		registeredDriver = driver;
	}

	/**
	 * According to JDBC specification, this driver is registered against
	 * {@link DriverManager} when the class is loaded. To avoid leaks, this method
	 * allow unregistering the driver so that the class can be gc'ed if necessary.
	 *
	 * @throws IllegalStateException if the driver is not registered
	 * @throws SQLException          if deregistering the driver fails
	 */
	public static void deregister() throws SQLException {
		if (registeredDriver == null) {
			throw new IllegalStateException(
					"Driver is not registered (or it has not been registered using Driver.register() method)");
		}
		try {
			clear();
		} catch (Exception e) {
			throw new SQLException("Could not clear", e);
		}
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	public static void clear() throws Exception {
		ruleSessionManager.close();
		registryManager.close();
		queryCaches.clear();
		clientManager.close();
	}

	public static boolean isRegistered() {
		return registeredDriver != null;
	}

	public static String crc32(String string) {
		return Long.toHexString(HashingFunctions.crc32(string));
	}

}
