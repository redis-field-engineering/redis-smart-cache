package com.redis.smartcache;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.DriverConfig;
import com.redis.smartcache.core.Config.RedisConfig;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.ConfigManager;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.core.RedisResultSetCache;
import com.redis.smartcache.core.ResultSetCache;
import com.redis.smartcache.core.codec.ResultSetCodec;
import com.redis.smartcache.jdbc.SmartConnection;
import com.redis.smartcache.jdbc.rowset.CachedRowSetFactory;

import io.lettuce.core.AbstractRedisClient;
import io.micrometer.core.instrument.Clock;
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
	private static final Logger parentLogger = Logger.getLogger("com.redis.smartcache");
	private static final Logger logger = Logger.getLogger("com.redis.smartcache.Driver");

	public static final String PROPERTY_PREFIX = "smartcache";
	public static final String PROPERTY_PREFIX_DRIVER = PROPERTY_PREFIX + ".driver";
	public static final String PROPERTY_PREFIX_REDIS = PROPERTY_PREFIX + ".redis";
	public static final String CACHE_KEY_PREFIX = "cache";
	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final JavaPropsSchema PROPS_SCHEMA = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
	private static final JavaPropsMapper PROPS_MAPPER = JavaPropsMapper.builder()
			.propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE).serializationInclusion(Include.NON_NULL)
			.addModule(new JavaTimeModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
			.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).build();

	private static final RowSetFactory rowSetFactory = new CachedRowSetFactory();
	private static final Map<String, java.sql.Driver> drivers = new HashMap<>();
	private static final Map<Config, MeterRegistry> registries = new HashMap<>();
	private static final Map<Config, QueryRuleSession> sessions = new HashMap<>();
	private static final Map<Config, ConfigManager<RulesetConfig>> configManagers = new HashMap<>();
	private static final Map<RedisConfig, AbstractRedisClient> clients = new HashMap<>();

	static {
		try {
			register();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
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
		return PROPS_MAPPER.readPropertiesAs(properties, PROPS_SCHEMA, Config.class);
	}

	public static Properties properties(Config config) throws IOException {
		return PROPS_MAPPER.writeValueAsProperties(config, PROPS_SCHEMA);
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
			return null;
		}
		String redisUri = matcher.group(1);
		if (redisUri == null || redisUri.isEmpty()) {
			return null;
		}
		Config config;
		try {
			config = config(info);
		} catch (IOException e) {
			throw new SQLException("Could not load config", e);
		}
		config.getRedis().setUri(redisUri);
		return makeConnection(config, info);
	}

	private static SmartConnection makeConnection(Config conf, Properties info) throws SQLException {
		Connection backend = backendConnection(conf.getDriver(), info);
		AbstractRedisClient client = clients.computeIfAbsent(conf.getRedis(), Driver::redisClient);
		QueryRuleSession session = sessions.computeIfAbsent(conf, c -> ruleSession(c, client));
		ResultSetCodec codec = new ResultSetCodec(rowSetFactory, Math.toIntExact(conf.getCodecBufferSize().toBytes()));
		String prefix = conf.key(CACHE_KEY_PREFIX) + conf.getKeySeparator();
		ResultSetCache cache = new RedisResultSetCache(RedisModulesUtils.connection(client, codec), prefix);
		MeterRegistry registry = registries.computeIfAbsent(conf, c -> registry(c, client));
		return new SmartConnection(backend, session, rowSetFactory, cache, registry, conf);
	}

	private static RedisTimeSeriesMeterRegistry registry(Config conf, AbstractRedisClient client) {
		return new RedisTimeSeriesMeterRegistry(new MeterRegistryConfig(conf), Clock.SYSTEM, client);
	}

	private static Connection backendConnection(DriverConfig config, Properties info) throws SQLException {
		String url = config.getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver = backendDriver(config.getClassName());
		logger.log(Level.FINE, "Connecting to backend database with URL: {0}", url);
		return driver.connect(url, info);
	}

	public static synchronized java.sql.Driver backendDriver(String className) throws SQLException {
		if (className == null || className.isEmpty()) {
			throw new SQLException("No backend driver class specified");
		}
		if (drivers.containsKey(className)) {
			return drivers.get(className);
		}
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(className).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Could not load backend driver class '" + className + "'", e);
		}
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
		return parentLogger;
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
		drivers.clear();
		configManagers.values().forEach(ConfigManager::close);
		configManagers.clear();
		registries.values().forEach(MeterRegistry::close);
		registries.clear();
		sessions.clear();
		clients.values().forEach(c -> {
			c.shutdown();
			c.getResources().shutdown();
		});
		clients.clear();
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	public static boolean isRegistered() {
		return registeredDriver != null;
	}

	private static QueryRuleSession ruleSession(Config config, AbstractRedisClient redisClient) {
		QueryRuleSession ruleSession = QueryRuleSession.of(config.getRuleset());
		config.getRuleset().addPropertyChangeListener(ruleSession);
		if (configManagers.containsKey(config)) {
			throw new IllegalStateException(
					MessageFormat.format("Config manager already exists for config {0}", config));
		}
		StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(redisClient);
		try {
			configManagers.put(config,
					new ConfigManager<>(connection, config.key("config"), config.getRuleset(), config.getConfigStep()));
		} catch (JsonProcessingException e) {
			logger.log(Level.WARNING, "Could not initialize config manager", e);
		}
		return ruleSession;
	}

	private static AbstractRedisClient redisClient(RedisConfig config) {
		RedisURIBuilder redisURI = RedisURIBuilder.create();
		redisURI.uri(config.getUri());
		redisURI.username(config.getUsername());
		redisURI.password(config.getPassword());
		redisURI.sslVerifyMode(config.getTls().getVerify());
		redisURI.ssl(config.getTls().isEnabled());
		return ClientBuilder.create(redisURI.build()).cluster(config.isCluster()).build();
	}

	private static class MeterRegistryConfig implements RedisTimeSeriesConfig {

		private final Config config;

		protected MeterRegistryConfig(Config config) {
			this.config = config;
		}

		@Override
		public String get(String key) {
			return null;
		}

		@Override
		public String uri() {
			return config.getRedis().getUri();
		}

		@Override
		public boolean cluster() {
			return config.getRedis().isCluster();
		}

		@Override
		public String keyspace() {
			return config.key("metrics");
		}

		@Override
		public Duration step() {
			return config.getMetricsStep();
		}

	}

}
