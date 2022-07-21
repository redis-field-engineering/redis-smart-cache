package com.redis.sidecar;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class MeterRegistryManager {

	private final Map<String, MeterRegistry> registries = new HashMap<>();

	public MeterRegistry getRegistry(Config config) {
		if (!registries.containsKey(config.getRedis().getUri())) {
			registries.put(config.getRedis().getUri(), registry(config));
		}
		return registries.get(config.getRedis().getUri());
	}

	private MeterRegistry registry(Config config) {
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
				return Duration.ofSeconds(config.getMetricsStep());
			}

		}, Clock.SYSTEM);
	}

	public void clear() {
		registries.forEach((k, v) -> v.close());
		registries.clear();
	}

}
