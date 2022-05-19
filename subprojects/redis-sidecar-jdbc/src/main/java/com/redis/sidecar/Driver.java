package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.core.ByteArrayResultSetCodec;
import com.redis.sidecar.core.Config;
import com.redis.sidecar.core.Config.Redis;
import com.redis.sidecar.core.Config.Redis.Pool;
import com.redis.sidecar.core.ConfigUpdater;
import com.redis.sidecar.core.ResultSetCache;
import com.redis.sidecar.core.StringResultSetCache;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;

public class Driver implements java.sql.Driver {

	private static final Logger log = Logger.getLogger(Driver.class.getName());

	public static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	public static final String PROPERTY_PREFIX = "sidecar";

	public static final String KEYSPACE = "sidecar";
	public static final char KEY_SEPARATOR = ':';

	static {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private Map<String, AbstractRedisClient> redisClients = new HashMap<>();

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		Properties properties = new Properties();
		properties.putAll(System.getenv());
		properties.putAll(System.getProperties());
		properties.putAll(info);
		JavaPropsMapper propsMapper = new JavaPropsMapper();
		JavaPropsSchema propsSchema = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
		Config config;
		try {
			config = propsMapper.readPropertiesAs(info, propsSchema, Config.class);
		} catch (IOException e) {
			throw new SQLException("Could not load configuration", e);
		}
		config.getRedis().setUri(matcher.group(1));
		if (isEmpty(config.getDriver().getClassName())) {
			throw new SQLException("No backend driver class specified");
		}
		if (isEmpty(config.getDriver().getUrl())) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(config.getDriver().getClassName()).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + config.getDriver().getClassName() + "'", e);
		}
		AbstractRedisClient client = client(config.getRedis());
		ResultSetCache cache;
		try {
			cache = cache(client, config);
		} catch (JsonProcessingException e) {
			throw new SQLException("Cannot initialize ResultSet cache", e);
		}
		Connection connection = driver.connect(config.getDriver().getUrl(), info);
		return new SidecarConnection(connection, cache, configUpdater(client, config));
	}

	private ConfigUpdater configUpdater(AbstractRedisClient client, Config config) {
		return new ConfigUpdater(config.getRedis().isCluster() ? ((RedisModulesClusterClient) client).connect()
				: ((RedisModulesClient) client).connect(), key(config.getCacheName(), "config"), config);
	}

	public static String key(String... segments) {
		StringJoiner joiner = new StringJoiner(String.valueOf(KEY_SEPARATOR));
		joiner.add(KEYSPACE);
		for (String segment : segments) {
			joiner.add(segment);
		}
		return joiner.toString();
	}

	public static ResultSetCache cache(AbstractRedisClient client, Config config) throws JsonProcessingException {
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(config.getBufferSize());
		RedisTimeSeriesMeterRegistry meterRegistry = meterRegistry(config);
		String keyspace = keyspace(config);
		if (config.getRedis().isCluster()) {
			return new StringResultSetCache<>(meterRegistry, ConnectionPoolSupport
					.createGenericObjectPool(() -> ((RedisClusterClient) client).connect(codec), poolConfig(config)),
					StatefulRedisClusterConnection::sync, keyspace);
		}
		return new StringResultSetCache<>(
				meterRegistry, ConnectionPoolSupport
						.createGenericObjectPool(() -> ((RedisClient) client).connect(codec), poolConfig(config)),
				StatefulRedisConnection::sync, keyspace);
	}

	public static RedisTimeSeriesMeterRegistry meterRegistry(Config config) {
		return new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

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
				return Driver.keyspace(config);
			}

			@Override
			public Duration step() {
				return Duration.ofSeconds(config.getMetrics().getPublishInterval());
			}

		}, Clock.SYSTEM);
	}

	public static String keyspace(Config config) {
		return KEYSPACE + KEY_SEPARATOR + config.getCacheName();
	}

	private AbstractRedisClient client(Redis redis) {
		String redisURI = redis.getUri();
		if (redisClients.containsKey(redisURI)) {
			return redisClients.get(redisURI);
		}
		return redisClients.put(redisURI,
				redis.isCluster() ? RedisModulesClusterClient.create(redisURI) : RedisModulesClient.create(redisURI));
	}

	private static <T> GenericObjectPoolConfig<T> poolConfig(Config sidecarConfig) {
		Pool pool = sidecarConfig.getRedis().getPool();
		GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(pool.getMaxActive());
		config.setMaxIdle(pool.getMaxIdle());
		config.setMinIdle(pool.getMinIdle());
		config.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		config.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return config;
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
				new DriverPropertyInfo(PROPERTY_PREFIX + ".driver.url",
						info.getProperty(PROPERTY_PREFIX + ".driver.url")),
				new DriverPropertyInfo(PROPERTY_PREFIX + ".driver.class-name",
						info.getProperty(PROPERTY_PREFIX + ".driver.class-name")) };
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
