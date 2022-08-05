package com.redis.sidecar;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.redis.micrometer.RedisTimeSeriesConfig;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;

import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class MeterManager {

	private final Map<RedisURI, MeterRegistry> registries = new HashMap<>();

	public MeterRegistry getRegistry(Config config) {
		RedisURI uri = config.getRedis().uri();
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

	public void clear() {
		registries.forEach((k, v) -> v.close());
		registries.clear();
	}

}
