package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;
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
import com.redis.sidecar.codec.ExplicitResultSetCodec;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micrometer.core.instrument.MeterRegistry;

public class NonRegisteringDriver implements Driver {

	private static final Logger log = Logger.getLogger(NonRegisteringDriver.class.getName());

	private static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	private static final String PROPERTY_PREFIX = "sidecar";
	private static final String PROPERTY_DRIVER_PREFIX = PROPERTY_PREFIX + ".driver";

	private static BackendManager backendManager = new BackendManager();
	private static MeterManager meterManager = new MeterManager();
	private static RedisManager redisManager = new RedisManager(meterManager);
	private static ConfigManager configManager = new ConfigManager(redisManager);

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
		try {
			config = configManager.getConfig(config);
		} catch (JsonProcessingException e) {
			throw new SQLException("Could not initialize config object", e);
		}
		MeterRegistry meterRegistry = meterManager.getRegistry(config);
		AbstractRedisClient redisClient = redisManager.getClient(config);
		Connection backendConnection = backendManager.connect(config, info);
		RowSetFactory rowSetFactory = RowSetProvider.newFactory();
		ExplicitResultSetCodec codec = new ExplicitResultSetCodec(RowSetProvider.newFactory(), config.getBufferSize());
		GenericObjectPool<StatefulConnection<String, ResultSet>> pool = redisManager.getConnectionPool(config, codec);
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

	public static boolean isEmpty(String string) {
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
		meterManager.clear();
		redisManager.clear();
	}

}
