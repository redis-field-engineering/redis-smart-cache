package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.config.Config;
import com.redis.sidecar.config.Pool;
import com.redis.sidecar.core.ByteArrayResultSetCodec;
import com.redis.sidecar.core.ConfigUpdater;
import com.redis.sidecar.core.ResultSetCache;
import com.redis.sidecar.core.StringResultSetCache;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);

	private static final String PROPERTY_PREFIX = "sidecar";
	private static final String PROPERTY_DRIVER_PREFIX = PROPERTY_PREFIX + ".driver";

	public static final String PROPERTY_KEYSPACE = PROPERTY_PREFIX + ".keyspace";
	public static final String PROPERTY_CACHE_NAME = PROPERTY_PREFIX + ".cache-name";
	public static final String PROPERTY_METRICS_STEP = PROPERTY_PREFIX + ".metrics.step";
	public static final String PROPERTY_DRIVER_CLASSNAME = PROPERTY_DRIVER_PREFIX + ".class-name";
	public static final String PROPERTY_DRIVER_URL = PROPERTY_DRIVER_PREFIX + ".url";
	public static final String PROPERTY_CLUSTER = PROPERTY_PREFIX + ".cluster";

	public static final String KEY_SEPARATOR = ":";
	public static final String DEFAULT_KEYSPACE = "sidecar";
	public static final String DEFAULT_CACHE_NAME = "default";
	public static final Duration DEFAULT_METRICS_STEP = Duration.ofMinutes(1);

	static {
		try {
			DriverManager.registerDriver(new SidecarDriver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static AbstractRedisClient redisClient;
	private static ConfigUpdater configUpdater;

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Connection backendConnection = backendConnection(info);
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		String redisURI = matcher.group(1);
		boolean cluster = Boolean.parseBoolean(info.getProperty(PROPERTY_CLUSTER));
		String cacheName = info.getProperty(PROPERTY_CACHE_NAME, DEFAULT_CACHE_NAME);
		String keyspace = key(info.getProperty(PROPERTY_KEYSPACE, DEFAULT_KEYSPACE), cacheName);
		checkRedisClient(redisURI, cluster, keyspace);
		RowSetFactory rowSetFactory = RowSetProvider.newFactory();
		Config config = new Config();
		try {
			configUpdater.create(key(keyspace, "config"), config);
		} catch (JsonProcessingException e) {
			throw new SQLException("Could not initialize config object", e);
		}
		ResultSetCache cache = cache(redisClient, keyspace, rowSetFactory, config);
		return new SidecarConnection(backendConnection, keyspace, config, cache, rowSetFactory);
	}

	private static void checkRedisClient(String redisURI, boolean cluster, String keyspace) {
		if (redisClient == null) {
			redisClient = cluster ? RedisModulesClusterClient.create(redisURI) : RedisModulesClient.create(redisURI);
			LettuceAssert.isTrue(configUpdater == null,
					"Invalid state: Redis client is null but config updater is not");
			configUpdater = new ConfigUpdater(redisClient instanceof RedisModulesClusterClient
					? ((RedisModulesClusterClient) redisClient).connect()

					: ((RedisModulesClient) redisClient).connect());
			Metrics.addRegistry(new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

				@Override
				public String get(String key) {
					return null;
				}

				@Override
				public String uri() {
					return redisURI;
				}

				@Override
				public boolean cluster() {
					return cluster;
				}

				@Override
				public String keyspace() {
					return key(keyspace, "metrics");
				}

				@Override
				public Duration step() {
					return DEFAULT_METRICS_STEP;
				}

			}, Clock.SYSTEM, redisClient));
		}
	}

	private ResultSetCache cache(AbstractRedisClient client, String keyspace, RowSetFactory rowSetFactory,
			Config config) {
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(rowSetFactory, config.getBufferSize());
		boolean cluster = client instanceof RedisClusterClient;
		GenericObjectPool<StatefulConnection<String, ResultSet>> pool = ConnectionPoolSupport
				.createGenericObjectPool(cluster ? () -> ((RedisClusterClient) client).connect(codec)
						: () -> ((RedisClient) client).connect(codec), poolConfig(config));
		return new StringResultSetCache(key(keyspace, "cache"), pool, sync());
	}

	private static <T> GenericObjectPoolConfig<T> poolConfig(Config config) {
		Pool pool = config.getPool();
		GenericObjectPoolConfig<T> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(pool.getMaxActive());
		poolConfig.setMaxIdle(pool.getMaxIdle());
		poolConfig.setMinIdle(pool.getMinIdle());
		poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		poolConfig.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return poolConfig;
	}

	private static Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync() {
		if (redisClient instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<String, ResultSet>) c).sync();
		}
		return c -> ((StatefulRedisConnection<String, ResultSet>) c).sync();
	}

	private Connection backendConnection(Properties info) throws SQLException {
		String driverClassName = info.getProperty(PROPERTY_DRIVER_CLASSNAME);
		if (isEmpty(driverClassName)) {
			throw new SQLException("No backend driver class specified");
		}
		String driverURL = info.getProperty(PROPERTY_DRIVER_URL);
		if (isEmpty(driverURL)) {
			throw new SQLException("No backend URL specified");
		}
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(driverClassName).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + driverClassName + "'", e);
		}
		return driver.connect(driverURL, info);
	}

	public static Config config(Properties info) throws IOException {
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

	public static void shutdown() throws InterruptedException, ExecutionException {
		if (configUpdater != null) {
			configUpdater.close();
		}
		if (redisClient != null) {
			redisClient.shutdown();
			redisClient.getResources().shutdown().get();
		}
	}

	public static String key(String keyspace, String id) {
		return keyspace + KEY_SEPARATOR + id;
	}

}
