package com.redis.sidecar;

import java.time.Duration;
import java.util.Objects;

import com.redis.micrometer.RedisTimeSeriesConfig;

public class MeterRegistryConfig implements RedisTimeSeriesConfig {

	private final String uri;
	private final boolean cluster;
	private final String keyspace;
	private final Duration step;

	public MeterRegistryConfig(BootstrapConfig bootstrap) {
		this.uri = bootstrap.getRedis().getUri();
		this.cluster = bootstrap.getRedis().isCluster();
		this.keyspace = bootstrap.key("metrics");
		this.step = bootstrap.getMetricsStep();
	}

	@Override
	public String get(String key) {
		return null;
	}

	@Override
	public String uri() {
		return uri;
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

	@Override
	public int hashCode() {
		return Objects.hash(keyspace, uri);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MeterRegistryConfig other = (MeterRegistryConfig) obj;
		return Objects.equals(keyspace, other.keyspace) && Objects.equals(uri, other.uri);
	}

}
