package com.redis.smartcache;

import static com.redis.smartcache.core.RedisResultSetCache.METER_QUERY;
import static com.redis.smartcache.core.RedisResultSetCache.TAG_SQL;
import static com.redis.smartcache.core.RedisResultSetCache.TAG_TABLE;

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
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.micrometer.RediSearchMeterRegistry;
import com.redis.micrometer.RediSearchRegistryConfig;
import com.redis.micrometer.RedisRegistryConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.DriverConfig;
import com.redis.smartcache.core.Config.RedisConfig;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.ConfigManager;
import com.redis.smartcache.core.EvictingLinkedHashMap;
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.core.RedisResultSetCache;
import com.redis.smartcache.core.ResultSetCache;
import com.redis.smartcache.core.RuntimeSQLException;
import com.redis.smartcache.core.codec.ResultSetCodec;
import com.redis.smartcache.jdbc.SmartConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

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

	private final Map<RedisConfig, AbstractRedisClient> clients = new HashMap<>();
	private final Map<Config, MeterRegistry> registries = new HashMap<>();
	private final Map<Config, QueryRuleSession> sessions = new HashMap<>();
	private final Map<Config, ConfigManager<RulesetConfig>> configManagers = new HashMap<>();
	private final Map<Config, Map<String, Query>> queryCaches = new HashMap<>();

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
		configManager(config);
		Connection backendConnection = backendConnection(config.getDriver(), info);
		log.fine("Creating SmartCache connection");
		return makeConnection(config, backendConnection);
	}

	private SmartConnection makeConnection(Config config, Connection backendConnection) {
		return new SmartConnection(backendConnection, resultSetCache(config));
	}

	private ResultSetCache resultSetCache(Config config) {
		QueryRuleSession session = sessions.computeIfAbsent(config, this::ruleSession);
		KeyBuilder keyBuilder = keyBuilder(config, KEYSPACE_CACHE);
		MeterRegistry registry = registries.computeIfAbsent(config, this::createMeterRegistry);
		AbstractRedisClient client = client(config);
		RedisCodec<String, ResultSet> codec = resultSetCodec(config);
		StatefulRedisModulesConnection<String, ResultSet> connection = RedisModulesUtils.connection(client, codec);
		Map<String, Query> queryCache = queryCache(config);
		return new RedisResultSetCache(connection, registry, keyBuilder, session, queryCache);
	}

	private ConfigManager<RulesetConfig> configManager(Config config) {
		return configManagers.computeIfAbsent(config, this::createConfigManager);
	}

	private ConfigManager<RulesetConfig> createConfigManager(Config config) {
		String key = keyBuilder(config).create(KEY_CONFIG);
		AbstractRedisClient redisClient = client(config);
		Duration period = duration(config.getRuleset().getRefresh());
		log.log(Level.FINE, "Creating config manager on key {0} with refresh interval {1}",
				new Object[] { key, period });
		try {
			return new ConfigManager<>(redisClient, JSON_MAPPER, key, config.getRuleset(), period);
		} catch (JsonProcessingException e) {
			throw new RuntimeSQLException("Could not create config manager", e);
		}
	}

	private Map<String, Query> queryCache(Config config) {
		return queryCaches.computeIfAbsent(config, this::createQueryCache);
	}

	private Map<String, Query> createQueryCache(Config config) {
		return new EvictingLinkedHashMap<>(config.getAnalyzer().getCacheCapacity());
	}

	private static RedisCodec<String, ResultSet> resultSetCodec(Config config) {
		int bufferSize = Math.toIntExact(config.getRedis().getCodecBufferCapacity().toBytes());
		return new ResultSetCodec(bufferSize);
	}

	public static KeyBuilder keyBuilder(Config config, String prefix) {
		log.log(Level.FINE, "Creating KeyBuilder for prefix {0}", prefix);
		return keyBuilder(config).builder(prefix);
	}

	private static KeyBuilder keyBuilder(Config config) {
		return new KeyBuilder(config.getName(), config.getRedis().getKeySeparator());
	}

	private AbstractRedisClient client(Config config) {
		return clients.computeIfAbsent(config.getRedis(), this::createRedisClient);
	}

	private Duration duration(io.airlift.units.Duration duration) {
		return Duration.ofMillis(duration.toMillis());
	}

	private MeterRegistry createMeterRegistry(Config config) {
		AbstractRedisClient client = client(config);
		log.fine("Creating meter registry");
		KeyBuilder keyBuilder = keyBuilder(config);
		Duration step = duration(config.getMetrics().getStep());
		String tsKeyspace = keyBuilder.create(KEYSPACE_METRICS);
		RedisTimeSeriesMeterRegistry tsRegistry = new RedisTimeSeriesMeterRegistry(new RedisRegistryConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public String keyspace() {
				return tsKeyspace;
			}

			@Override
			public String keySeparator() {
				return keyBuilder.getSeparator();
			}

			@Override
			public Duration step() {
				return step;
			}
		}, Clock.SYSTEM, client);
		tsRegistry.config().meterFilter(MeterFilter.ignoreTags(TAG_SQL, TAG_TABLE));
		String searchKeyspace = keyBuilder.getKeyspace();
		RediSearchMeterRegistry searchRegistry = new RediSearchMeterRegistry(new RediSearchRegistryConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public String keyspace() {
				return searchKeyspace;
			}

			@Override
			public String keySeparator() {
				return keyBuilder.getSeparator();
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public String[] nonKeyTags() {
				return new String[] { TAG_SQL, TAG_TABLE };
			}

			@Override
			public String indexPrefix() {
				return keyBuilder.getKeyspace();
			}

			@Override
			public String indexSuffix() {
				return "idx";
			}

		}, Clock.SYSTEM, client);
		searchRegistry.config().meterFilter(MeterFilter.acceptNameStartsWith(METER_QUERY))
				.meterFilter(MeterFilter.deny());
		return new CompositeMeterRegistry().add(tsRegistry).add(searchRegistry);
	}

	private Connection backendConnection(DriverConfig config, Properties info) throws SQLException {
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
		registeredDriver.clear();
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	public void clear() {
		configManagers.values().forEach(ConfigManager::close);
		configManagers.clear();
		registries.values().forEach(MeterRegistry::close);
		registries.clear();
		queryCaches.clear();
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
		log.fine("Creating rule session");
		QueryRuleSession ruleSession = QueryRuleSession.of(config.getRuleset());
		config.getRuleset().addPropertyChangeListener(ruleSession);
		return ruleSession;
	}

	private RedisURI redisURI(RedisConfig config) {
		RedisURIBuilder builder = RedisURIBuilder.create();
		builder.uri(config.getUri());
		builder.username(config.getUsername());
		builder.password(config.getPassword());
		builder.ssl(config.isTls());
		builder.sslVerifyMode(config.getTlsVerify());
		return builder.build();
	}

	private AbstractRedisClient createRedisClient(RedisConfig config) {
		RedisURI redisURI = redisURI(config);
		log.log(Level.FINE, "Creating Redis client with URI {0}", redisURI);
		return ClientBuilder.create(redisURI).cluster(config.isCluster()).build();
	}

}
