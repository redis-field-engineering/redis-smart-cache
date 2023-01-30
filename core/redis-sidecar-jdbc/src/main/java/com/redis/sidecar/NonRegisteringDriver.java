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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.BootstrapConfig.Redis;
import com.redis.sidecar.BootstrapConfig.Redis.Pool;
import com.redis.sidecar.codec.ResultSetCodec;
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
	private static final Map<String, AbstractRedisClient> clients = new HashMap<>();
	private static final Map<AbstractRedisClient, GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>>> pools = new HashMap<>();
	private static final Map<String, MeterRegistry> registries = new HashMap<>();

	public NonRegisteringDriver() {
		// Needed for Class.forName().newInstance()
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		final BootstrapConfig bootstrap;
		try {
			bootstrap = loadConfig(info, BootstrapConfig.class);
		} catch (IOException e) {
			throw new SQLException("Could not load config", e);
		}
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		RedisURI redisURI = redisURI(matcher.group(1), bootstrap.getRedis());
		MeterRegistryConfig registryConfig = registryConfig(redisURI, bootstrap);
		MeterRegistry meterRegistry = registries.computeIfAbsent(url,
				u -> new RedisTimeSeriesMeterRegistry(registryConfig, Clock.SYSTEM));
		AbstractRedisClient client = clients.computeIfAbsent(url, u -> client(redisURI, bootstrap, meterRegistry));
		StatefulRedisModulesConnection<String, String> connection = connection(client);
		final Config config;
		try {
			config = configManager.fetchConfig(connection, bootstrap.getRedis().key("config"),
					Duration.ofSeconds(bootstrap.getRefreshRate()), loadConfig(info, Config.class));
		} catch (IOException e) {
			throw new SQLException("Could not initialize config object", e);
		}
		Connection backendConnection = connect(bootstrap, info);
		RowSetFactory rowSetFactory = new SidecarRowSetFactory();
		ResultSetCodec codec = ResultSetCodec.builder().maxByteBufferCapacity(config.getBufferSize()).build();
		Pool poolConfig = bootstrap.getRedis().getPool();
		GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool = pools.computeIfAbsent(client,
				c -> pool(client, codec, poolConfig));
		ResultSetCache cache = new ResultSetCacheImpl(meterRegistry, pool, bootstrap.getRedis().key("cache"));
		return new SidecarConnection(backendConnection, config, cache, rowSetFactory, meterRegistry);
	}

	private MeterRegistryConfig registryConfig(RedisURI redisURI, BootstrapConfig config) {
		return new MeterRegistryConfig(redisURI, config.getRedis().isCluster(), config.getRedis().key("metrics"),
				Duration.ofSeconds(config.getMetrics().getStep()));
	}

	private Connection connect(BootstrapConfig config, Properties info) throws SQLException {
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

	private <T> T loadConfig(Properties info, Class<T> type) throws IOException {
		Properties properties = new Properties();
		properties.putAll(System.getenv());
		properties.putAll(System.getProperties());
		properties.putAll(info);
		JavaPropsMapper propsMapper = new JavaPropsMapper();
		propsMapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		propsMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		JavaPropsSchema propsSchema = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
		return propsMapper.readPropertiesAs(properties, propsSchema, type);
	}

	@SuppressWarnings("deprecation")
	private RedisURI redisURI(String uri, Redis redis) {
		RedisURI redisURI = RedisURI.create(uri);
		redisURI.setVerifyPeer(!redis.isInsecure());
		if (redis.isTls()) {
			redisURI.setSsl(redis.isTls());
		}
		if (redis.getUsername() != null) {
			redisURI.setUsername(redis.getUsername());
		}
		if (redis.getPassword() != null) {
			redisURI.setPassword(redis.getPassword());
		}
		return redisURI;
	}

	private AbstractRedisClient client(RedisURI uri, BootstrapConfig config, MeterRegistry meterRegistry) {
		Builder builder = ClientResources.builder();
		if (config.getMetrics().isLettuce()) {
			builder.commandLatencyRecorder(
					new MicrometerCommandLatencyRecorder(meterRegistry, MicrometerOptions.create()));
		}
		ClientResources resources = builder.build();
		return config.getRedis().isCluster() ? RedisModulesClusterClient.create(resources, uri)
				: RedisModulesClient.create(resources, uri);
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
		drivers.clear();
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
