package com.redis.sidecar.core;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.core.config.Config;

import io.lettuce.core.AbstractRedisClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class MeterRegistryManager {

	public static final Duration DEFAULT_METRICS_STEP = Duration.ofMinutes(1);

	private final Map<AbstractRedisClient, MeterRegistry> registries = new HashMap<>();

	public MeterRegistry getRegistry(AbstractRedisClient redisClient, Config config) {
		if (!registries.containsKey(redisClient)) {
			registries.put(redisClient, registry(redisClient, config));
		}
		return registries.get(redisClient);
	}

	private MeterRegistry registry(AbstractRedisClient redisClient, Config config) {
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
				return config.getRedis().key("metrics");
			}

			@Override
			public Duration step() {
				return DEFAULT_METRICS_STEP;
			}

		}, Clock.SYSTEM, redisClient);
	}

}
