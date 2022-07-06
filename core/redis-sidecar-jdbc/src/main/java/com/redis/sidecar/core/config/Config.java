package com.redis.sidecar.core.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class Config {

	public static final Duration DEFAULT_REFRESH_RATE = Duration.ofSeconds(10);

	private long refreshRate = DEFAULT_REFRESH_RATE.toMillis();
	private Driver driver = new Driver();
	private Redis redis = new Redis();
	private List<Rule> rules = Arrays.asList(Rule.builder().build());

	public long getRefreshRate() {
		return refreshRate;
	}

	public void setRefreshRate(long refreshRate) {
		this.refreshRate = refreshRate;
	}

	public Driver getDriver() {
		return driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
	}

	public Redis getRedis() {
		return redis;
	}

	public void setRedis(Redis redis) {
		this.redis = redis;
	}

	public List<Rule> getRules() {
		return rules;
	}

	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}

}
