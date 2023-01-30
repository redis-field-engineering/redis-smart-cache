package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetFactory;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.Config.Pool;
import com.redis.sidecar.Config.Redis;
import com.redis.sidecar.rowset.SidecarRowSetFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.ClientResources.Builder;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class NonRegisteringDriver implements Driver {

	private static final Logger log = Logger.getLogger(NonRegisteringDriver.class.getName());

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final String PROPERTY_PREFIX = "sidecar";
	private static final String PROPERTY_DRIVER_PREFIX = PROPERTY_PREFIX + ".driver";

	private static final Map<String, Driver> drivers = new HashMap<>();
	private static final ConfigManager configManager = new ConfigManager();
	private static final Map<RedisURI, AbstractRedisClient> clients = new HashMap<>();
	private static final Map<AbstractRedisClient, GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>>> pools = new HashMap<>();
	private static final Map<String, MeterRegistry> registries = new HashMap<>();

	private MeterRegistry getRegistry(Config config) {
		String uri = config.getRedis().getUri();
		if (registries.containsKey(uri)) {
			return registries.get(uri);
		}
		MeterRegistry registry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

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
				return config.getRedis().key("metrics");
			}

			@Override
			public Duration step() {
				return Duration.ofSeconds(config.getMetrics().getStep());
			}

		}, Clock.SYSTEM);
		registries.put(uri, registry);
		return registry;
	}

	public NonRegisteringDriver() {
		// Needed for Class.forName().newInstance()
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Config config;
		try {
			config = config(info);
		} catch (IOException e) {
			throw new SQLException("Could not load config", e);
		}
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		config.getRedis().setUri(matcher.group(1));
		AbstractRedisClient redisClient = getClient(config);
		StatefulRedisModulesConnection<String, String> connection = connection(redisClient);
		String configKey = config.getRedis().key("config");
		try {
			config = configManager.getConfig(configKey, connection, config);
		} catch (JsonProcessingException e) {
			throw new SQLException("Could not initialize config object", e);
		}
		MeterRegistry meterRegistry = getRegistry(config);
		Connection backendConnection = connect(config, info);
		RowSetFactory rowSetFactory = new SidecarRowSetFactory();
		ResultSetCodec codec = ResultSetCodec.builder().maxByteBufferCapacity(config.getBufferSize()).build();
		GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool = getConnectionPool(redisClient,
				codec, config);
		ResultSetCache cache = new ResultSetCacheImpl(config, meterRegistry, pool);
		return new SidecarConnection(backendConnection, config, cache, rowSetFactory, meterRegistry);
	}

	private Connection connect(Config config, Properties info) throws SQLException {
		String className = config.getDriver().getClassName();
		if (className == null || className.isEmpty()) {
			throw new SQLException("No backend driver class specified");
		}
		String url = config.getDriver().getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		Driver driver;
		if (drivers.containsKey(className)) {
			driver = drivers.get(className);
		} else {
			try {
				driver = (Driver) Class.forName(className).getConstructor().newInstance();
			} catch (Exception e) {
				throw new SQLException("Cannot initialize backend driver '" + className + "'", e);
			}
			drivers.put(className, driver);
		}
		return driver.connect(url, info);
	}

	private Config config(Properties info) throws IOException {
		Properties properties = new Properties();
		properties.putAll(System.getenv());
		properties.putAll(System.getProperties());
		properties.putAll(info);
		JavaPropsMapper propsMapper = new JavaPropsMapper();
		propsMapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		propsMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		JavaPropsSchema propsSchema = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
		return propsMapper.readPropertiesAs(properties, propsSchema, Config.class);
	}

	@SuppressWarnings("deprecation")
	private RedisURI uri(Redis config) {
		RedisURI redisURI = RedisURI.create(config.getUri());
		redisURI.setVerifyPeer(!config.isInsecure());
		if (config.isTls()) {
			redisURI.setSsl(config.isTls());
		}
		if (config.getUsername() != null) {
			redisURI.setUsername(config.getUsername());
		}
		if (config.getPassword() != null) {
			redisURI.setPassword(config.getPassword());
		}
		return redisURI;
	}

	private AbstractRedisClient getClient(Config config) {
		Redis redis = config.getRedis();
		RedisURI uri = uri(redis);
		if (clients.containsKey(uri)) {
			return clients.get(uri);
		}
		Builder builder = ClientResources.builder();
		if (config.getMetrics().isLettuce()) {
			builder = builder.commandLatencyRecorder(
					new MicrometerCommandLatencyRecorder(getRegistry(config), MicrometerOptions.create()));
		}
		ClientResources resources = builder.build();
		AbstractRedisClient client = redis.isCluster() ? RedisModulesClusterClient.create(resources, uri)
				: RedisModulesClient.create(resources, uri);
		clients.put(uri, client);
		return client;
	}

	private GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> getConnectionPool(
			AbstractRedisClient client, RedisCodec<String, ResultSet> codec, Config config) {
		return pools.computeIfAbsent(client, c -> pool(client, codec, config.getRedis().getPool()));
	}

	private GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool(AbstractRedisClient client,
			RedisCodec<String, ResultSet> codec, Pool config) {
		return ConnectionPoolSupport.createGenericObjectPool(
				() -> client instanceof RedisModulesClusterClient ? ((RedisModulesClusterClient) client).connect(codec)
						: ((RedisModulesClient) client).connect(codec),
				poolConfig(config));
	}

	private GenericObjectPoolConfig<StatefulRedisModulesConnection<String, ResultSet>> poolConfig(Pool pool) {
		GenericObjectPoolConfig<StatefulRedisModulesConnection<String, ResultSet>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(pool.getMaxActive());
		config.setMaxIdle(pool.getMaxIdle());
		config.setMinIdle(pool.getMinIdle());
		config.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		config.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return config;
	}

	private StatefulRedisModulesConnection<String, String> connection(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connect();
		}
		return ((RedisModulesClient) client).connect();
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
		configManager.close();
		registries.forEach((k, v) -> v.close());
		registries.clear();
		pools.forEach((k, v) -> v.close());
		pools.clear();
		clients.forEach((k, v) -> {
			v.shutdown();
			v.getResources().shutdown();
		});
		clients.clear();
	}

}
