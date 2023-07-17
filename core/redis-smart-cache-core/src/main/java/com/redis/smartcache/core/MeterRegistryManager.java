package com.redis.smartcache.core;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.micrometer.RediSearchMeterRegistry;
import com.redis.micrometer.RediSearchRegistryConfig;
import com.redis.micrometer.RedisRegistryConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.smartcache.core.config.Config;

import io.lettuce.core.AbstractRedisClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

public class MeterRegistryManager implements AutoCloseable {

	private static final Logger log = Logger.getLogger(MeterRegistryManager.class.getName());

	public static final String KEYSPACE_METRICS = "metrics";

	private final ClientManager clientManager;
	private final Map<Config, MeterRegistry> registries = new HashMap<>();

	public MeterRegistryManager(ClientManager clientManager) {
		this.clientManager = clientManager;
	}

	public MeterRegistry getRegistry(Config config) {
		return registries.computeIfAbsent(config, this::meterRegistry);
	}

	private MeterRegistry meterRegistry(Config config) {
		switch (config.getMetrics().getRegistry()) {
		case JMX:
			return jmxMeterRegistry(config);
		case REDIS:
			return redisMeterRegistry(config);
		default:
			return simpleMeterRegistry(config);
		}
	}

	private SimpleMeterRegistry simpleMeterRegistry(Config config) {
		Duration step = step(config);
		return new SimpleMeterRegistry(new SimpleConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public Duration step() {
				return step;
			}

		}, Clock.SYSTEM);
	}

	private JmxMeterRegistry jmxMeterRegistry(Config config) {
		Duration step = step(config);
		return new JmxMeterRegistry(new JmxConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public String domain() {
				return config.getName();
			}

		}, Clock.SYSTEM);
	}

	private Duration duration(io.airlift.units.Duration duration) {
		return Duration.ofMillis(duration.toMillis());
	}

	private Duration step(Config config) {
		return duration(config.getMetrics().getStep());
	}

	private MeterRegistry redisMeterRegistry(Config config) {
		AbstractRedisClient client = clientManager.getClient(config);
		StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
		try {
			connection.sync().ftList();
		} catch (Exception e) {
			// Looks like we don't have Redis Modules
			log.info("No RediSearch found, downgrading to simple meter registry");
			return simpleMeterRegistry(config);
		}
		log.fine("Creating meter registry");
		Duration step = step(config);
		KeyBuilder keyBuilder = KeyBuilder.of(config);
		String tsKeyspace = keyBuilder.build(KEYSPACE_METRICS);
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
				return keyBuilder.separator();
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public boolean enabled() {
				return config.getMetrics().isEnabled();
			}

		}, Clock.SYSTEM, client);
		tsRegistry.config().meterFilter(MeterFilter.ignoreTags(Fields.TAG_SQL, Fields.TAG_TABLE));
		RediSearchMeterRegistry searchRegistry = new RediSearchMeterRegistry(new RediSearchRegistryConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public String keyspace() {
				return keyBuilder.keyspace().orElse(null);
			}

			@Override
			public String keySeparator() {
				return keyBuilder.separator();
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public String[] nonKeyTags() {
				return new String[] { Fields.TAG_SQL, Fields.TAG_TABLE, Fields.TAG_TYPE };
			}

			@Override
			public String indexPrefix() {
				return keyBuilder.keyspace().orElse(null);
			}

			@Override
			public String indexSuffix() {
				return "idx";
			}

			@Override
			public boolean enabled() {
				return config.getMetrics().isEnabled();
			}

		}, Clock.SYSTEM, client);
		searchRegistry.config().meterFilter(MeterFilter.acceptNameStartsWith(Fields.METER_QUERY))
				.meterFilter(MeterFilter.deny());
		return new CompositeMeterRegistry().add(tsRegistry).add(searchRegistry);
	}

	@Override
	public void close() throws Exception {
		registries.values().forEach(MeterRegistry::close);
		registries.clear();
	}

}
