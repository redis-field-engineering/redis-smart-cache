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
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.sidecar.core.ByteArrayResultSetCodec;
import com.redis.sidecar.core.ConfigManager;
import com.redis.sidecar.core.MeterRegistryManager;
import com.redis.sidecar.core.RedisManager;
import com.redis.sidecar.core.ResultSetCache;
import com.redis.sidecar.core.StringResultSetCache;
import com.redis.sidecar.core.config.Config;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micrometer.core.instrument.MeterRegistry;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final String PROPERTY_PREFIX = "sidecar";
	private static final String PROPERTY_DRIVER_PREFIX = PROPERTY_PREFIX + ".driver";

	static {
		try {
			DriverManager.registerDriver(new SidecarDriver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static RedisManager redisManager = new RedisManager();
	private static ConfigManager configManager = new ConfigManager();
	private static MeterRegistryManager meterRegistryManager = new MeterRegistryManager();

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
		AbstractRedisClient redisClient = redisManager.getClient(config.getRedis());
		Connection backendConnection = backendConnection(config, info);
		RowSetFactory rowSetFactory = RowSetProvider.newFactory();
		try {
			configManager.register(redisClient, config.getRedis().key("config"), config,
					Duration.ofMillis(config.getRefreshRate()));
		} catch (JsonProcessingException e) {
			throw new SQLException("Could not initialize config object", e);
		}
		MeterRegistry meterRegistry = meterRegistryManager.getRegistry(redisClient, config);
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(RowSetProvider.newFactory(),
				config.getRedis().getBufferSize(), meterRegistry);
		GenericObjectPool<StatefulConnection<String, ResultSet>> pool = redisManager
				.getConnectionPool(config.getRedis(), codec);
		ResultSetCache cache = new StringResultSetCache(config, meterRegistry, pool, sync(redisClient));
		return new SidecarConnection(backendConnection, config, cache, rowSetFactory, meterRegistry);
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

	private static Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync(
			AbstractRedisClient redisClient) {
		if (redisClient instanceof RedisClusterClient) {
			return c -> ((StatefulRedisClusterConnection<String, ResultSet>) c).sync();
		}
		return c -> ((StatefulRedisConnection<String, ResultSet>) c).sync();
	}

	private Connection backendConnection(Config config, Properties info) throws SQLException {
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
		return driver.connect(config.getDriver().getUrl(), info);
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
		meterRegistryManager.clear();
		redisManager.clear();
	}

}
