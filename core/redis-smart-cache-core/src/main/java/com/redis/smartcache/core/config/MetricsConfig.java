package com.redis.smartcache.core.config;

import java.util.concurrent.TimeUnit;

import io.airlift.units.Duration;

public class MetricsConfig {

	public static final Duration DEFAULT_STEP = new Duration(60, TimeUnit.SECONDS);

	private boolean enabled = true;
	private MetricsRegistry registry = MetricsRegistry.REDIS;
	private Duration step = DEFAULT_STEP;

	/**
	 * 
	 * @return metrics publishing interval
	 */
	public Duration getStep() {
		return step;
	}

	public void setStep(Duration duration) {
		this.step = duration;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public MetricsRegistry getRegistry() {
		return registry;
	}

	public void setRegistry(MetricsRegistry registry) {
		this.registry = registry;
	}
}