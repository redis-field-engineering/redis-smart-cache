package com.redis.sidecar;

import java.time.Duration;

import com.redis.micrometer.RedisTimeSeriesConfig;

import io.lettuce.core.RedisURI;

public class MeterRegistryConfig implements RedisTimeSeriesConfig {

	private final RedisURI uri;
	private final boolean cluster;
	private final String keyspace;
	private final Duration step;

	public MeterRegistryConfig(RedisURI uri, boolean cluster, String keyspace, Duration step) {
		this.uri = uri;
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.step = step;
	}

	@Override
	public String get(String key) {
		return null;
	}

	@Override
	public String uri() {
		return uri.toString();
	}

	@Override
	public boolean cluster() {
		return cluster;
	}

	@Override
	public String keyspace() {
		return keyspace;
	}

	@Override
	public Duration step() {
		return step;
	}

}
