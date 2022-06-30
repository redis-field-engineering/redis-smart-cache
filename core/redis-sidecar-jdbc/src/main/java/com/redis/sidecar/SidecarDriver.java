package com.redis.sidecar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.json.SetMode;
import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.core.Config;
import com.redis.sidecar.core.ConfigUpdater;
import com.redis.sidecar.jdbc.SidecarConnection;

import io.lettuce.core.AbstractRedisClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class SidecarDriver implements Driver {

	private static final Logger log = Logger.getLogger(SidecarDriver.class.getName());

	public static final String JDBC_URL_REGEX = "jdbc\\:(rediss?(\\-(socket|sentinel))?\\:\\/\\/.*)";
	private static final Pattern JDBC_URL_PATTERN = Pattern.compile(JDBC_URL_REGEX);
	public static final String PROPERTY_PREFIX = "sidecar";

	static {
		try {
			DriverManager.registerDriver(new SidecarDriver());
		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static final Map<String, AbstractRedisClient> redisClients = new HashMap<>();
	private static final Map<String, MeterRegistry> meterRegistries = new HashMap<>();
	private static final Map<String, ConfigUpdater> configUpdaters = new HashMap<>();

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Config config;
		try {
			config = config(info);
		} catch (IOException e) {
			throw new SQLException("Could not load configuration", e);
		}
		if (isEmpty(config.getDriver().getClassName())) {
			throw new SQLException("No backend driver class specified");
		}
		if (isEmpty(config.getDriver().getUrl())) {
			throw new SQLException("No backend URL specified");
		}
		Matcher matcher = JDBC_URL_PATTERN.matcher(url);
		if (!matcher.find()) {
			throw new SQLException("Invalid connection URL: " + url);
		}
		String redisURI = matcher.group(1);
		AbstractRedisClient client;
		if (redisClients.containsKey(redisURI)) {
			client = redisClients.get(redisURI);
		} else {
			client = config.getRedis().isCluster() ? RedisModulesClusterClient.create(redisURI)
					: RedisModulesClient.create(redisURI);
			redisClients.put(redisURI, client);
		}
		String configId = redisURI + ":" + config.getCacheName();
		if (configUpdaters.containsKey(configId)) {
			config = configUpdaters.get(configId).getConfig();
		} else {
			try {
				ObjectWriter writer = mapper.writerFor(config.getClass());
				StatefulRedisModulesConnection<String, String> connection = client instanceof RedisModulesClusterClient
						? ((RedisModulesClusterClient) client).connect()
						: ((RedisModulesClient) client).connect();
				connection.sync().jsonSet(config.configKey(), "$", writer.writeValueAsString(config), SetMode.NX);
				configUpdaters.put(configId, new ConfigUpdater(connection, mapper.readerForUpdating(config), config));
			} catch (JsonProcessingException e) {
				throw new SQLException("Could not initialize config updater", e);
			}
		}
		return new SidecarConnection(databaseConnection(config, info), client, config,
				meterRegistry(redisURI, config, client));
	}

	private Connection databaseConnection(Config config, Properties info) throws SQLException {
		java.sql.Driver driver;
		try {
			driver = (java.sql.Driver) Class.forName(config.getDriver().getClassName()).getConstructor().newInstance();
		} catch (Exception e) {
			throw new SQLException("Cannot initialize backend driver '" + config.getDriver().getClassName() + "'", e);
		}
		return driver.connect(config.getDriver().getUrl(), info);
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

	private MeterRegistry meterRegistry(String redisURI, Config config, AbstractRedisClient client) {
		if (meterRegistries.containsKey(redisURI)) {
			return meterRegistries.get(redisURI);
		}
		MeterRegistry meterRegistry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

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
				return config.getRedis().isCluster();
			}

			@Override
			public String keyspace() {
				return config.key();
			}

			@Override
			public Duration step() {
				return Duration.ofSeconds(config.getMetrics().getPublishInterval());
			}

		}, Clock.SYSTEM, client);
		meterRegistries.put(redisURI, meterRegistry);
		return meterRegistry;
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

	public static void shutdown() {
		configUpdaters.values().forEach(ConfigUpdater::close);
		configUpdaters.clear();
		meterRegistries.values().forEach(MeterRegistry::close);
		meterRegistries.clear();
		redisClients.values().forEach(AbstractRedisClient::shutdown);
		redisClients.clear();
	}

}
