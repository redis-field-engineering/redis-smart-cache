package com.redis.smartcache;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
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
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.core.QueryWriter;
import com.redis.smartcache.core.RedisResultSetCache;
import com.redis.smartcache.core.ResultSetCache;
import com.redis.smartcache.core.codec.ResultSetCodec;
import com.redis.smartcache.jdbc.SmartConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
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
	public static final String KEYSPACE_QUERIES = "queries";
	public static final String KEYSPACE_METRICS = "metrics";
	public static final String KEYSPACE_CACHE = "cache";
	public static final String KEY_CONFIG = "config";
	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final JavaPropsSchema PROPS_SCHEMA = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
	private static final JavaPropsMapper PROPS_MAPPER = propsMapper();
	private static final JsonMapper JSON_MAPPER = jsonMapper();

	private static final Map<String, java.sql.Driver> drivers = new HashMap<>();
	private static final Map<RedisConfig, AbstractRedisClient> clients = new HashMap<>();
	private static final Map<Config, MeterRegistry> registries = new HashMap<>();
	private static final Map<Config, QueryRuleSession> sessions = new HashMap<>();
	private static final Map<Config, ConfigManager<RulesetConfig>> configManagers = new HashMap<>();
	private static final Map<Config, QueryWriter> queryWriters = new HashMap<>();

	public static JavaPropsMapper propsMapper() {
		return JavaPropsMapper.builder().serializationInclusion(Include.NON_NULL)
				.propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
	}

	public static JsonMapper jsonMapper() {
		return JsonMapper.builder().build();
	}

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
		Connection backendConnection = backendConnection(config.getDriver(), info);
		return makeConnection(config, backendConnection);
	}

	private SmartConnection makeConnection(Config conf, Connection backendConnection) {
		QueryRuleSession ruleSession = sessions.computeIfAbsent(conf, this::ruleSession);
		configManagers.computeIfAbsent(conf, this::configManager);
		KeyBuilder cacheKeyBuilder = keyBuilder(conf, KEYSPACE_CACHE);
		MeterRegistry meterRegistry = registries.computeIfAbsent(conf, this::createMeterRegistry);
		QueryWriter queryWriter = queryWriters.computeIfAbsent(conf, this::createQueryWriter);
		AbstractRedisClient client = redisClient(conf);
		RedisCodec<String, ResultSet> codec = resultSetCodec(conf);
		ResultSetCache resultSetCache = new RedisResultSetCache(RedisModulesUtils.connection(client, codec),
				meterRegistry, cacheKeyBuilder, ruleSession, queryWriter);
		return new SmartConnection(backendConnection, resultSetCache);
	}

	private RedisCodec<String, ResultSet> resultSetCodec(Config conf) {
		int bufferSize = Math.toIntExact(conf.getRedis().getCodecBufferCapacity().toBytes());
		return new ResultSetCodec(bufferSize);
	}

	private QueryWriter createQueryWriter(Config conf) {
		String index = conf.getRedis().getKey().getPrefix() + "-" + KEYSPACE_QUERIES + "-idx";
		KeyBuilder keyBuilder = keyBuilder(conf, KEYSPACE_QUERIES);
		return new QueryWriter(redisClient(conf), conf.getAnalyzer(), index, keyBuilder);
	}

	public static KeyBuilder keyBuilder(Config config) {
		return new KeyBuilder(config.getRedis().getKey().getPrefix(), config.getRedis().getKey().getSeparator());
	}

	public static KeyBuilder keyBuilder(Config config, String prefix) {
		return keyBuilder(config).builder(prefix);
	}

	private AbstractRedisClient redisClient(Config conf) {
		return clients.computeIfAbsent(conf.getRedis(), this::createRedisClient);
	}

	private ConfigManager<RulesetConfig> configManager(Config conf) {
		String key = keyBuilder(conf).create(KEY_CONFIG);
		AbstractRedisClient redisClient = redisClient(conf);
		Duration period = duration(conf.getRuleset().getRefresh());
		try {
			return new ConfigManager<>(redisClient, JSON_MAPPER, key, conf.getRuleset(), period);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Could not initialize config manager", e);
		}
	}

	private Duration duration(io.airlift.units.Duration duration) {
		return Duration.ofMillis(duration.toMillis());
	}

	private RedisTimeSeriesMeterRegistry createMeterRegistry(Config conf) {
		KeyBuilder keyBuilder = keyBuilder(conf).builder(KEYSPACE_METRICS);
		String keyPrefix = keyBuilder.create(""); // Registry expects prefix that includes trailing separator
		String keySeparator = keyBuilder.getSeparator();
		Duration step = duration(conf.getMetrics().getStep());
		RedisTimeSeriesConfig registryConfig = new RedisTimeSeriesConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public String keyPrefix() {
				return keyPrefix;
			}

			@Override
			public String keySeparator() {
				return keySeparator;
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public boolean enabled() {
				return conf.getMetrics().isEnabled();
			}
		};
		return new RedisTimeSeriesMeterRegistry(registryConfig, Clock.SYSTEM, redisClient(conf));
	}

	private static Connection backendConnection(DriverConfig config, Properties info) throws SQLException {
		Properties backendInfo = new Properties();
		for (String name : info.stringPropertyNames()) {
			if (name.startsWith(PROPS_SCHEMA.prefix())) {
				continue;
			}
			backendInfo.setProperty(name, info.getProperty(name));
		}
		String url = config.getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver = backendDriver(config.getClassName());
		logger.log(Level.FINE, "Connecting to backend database with URL: {0}", url);
		return driver.connect(url, backendInfo);
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
		clear();
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	public static void clear() {
		drivers.clear();
		configManagers.values().forEach(ConfigManager::close);
		configManagers.clear();
		registries.values().forEach(MeterRegistry::close);
		registries.clear();
		queryWriters.values().forEach(QueryWriter::close);
		queryWriters.clear();
		sessions.clear();
		clients.values().forEach(c -> {
			c.shutdown();
			c.getResources().shutdown();
		});
		clients.clear();
	}

	public static boolean isRegistered() {
		return registeredDriver != null;
	}

	private QueryRuleSession ruleSession(Config config) {
		QueryRuleSession ruleSession = QueryRuleSession.of(config.getRuleset());
		config.getRuleset().addPropertyChangeListener(ruleSession);
		return ruleSession;
	}

	private AbstractRedisClient createRedisClient(RedisConfig conf) {
		RedisURIBuilder redisURI = RedisURIBuilder.create();
		redisURI.uri(conf.getUri());
		redisURI.username(conf.getUsername());
		redisURI.password(conf.getPassword());
		redisURI.ssl(conf.isTls());
		redisURI.sslVerifyMode(conf.getTlsVerify());
		return ClientBuilder.create(redisURI.build()).cluster(conf.isCluster()).build();
	}

}
