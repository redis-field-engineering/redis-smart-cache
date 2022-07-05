package com.redis.sidecar.core.config;

import java.util.Arrays;
import java.util.List;

public class Config {

	public static final ByteSize DEFAULT_BUFFER_SIZE = ByteSize.ofMB(100);

	private Driver driver = new Driver();
	private Redis redis = new Redis();
	private int bufferSize = DEFAULT_BUFFER_SIZE.toBytes();
	private List<Rule> rules = Arrays.asList(Rule.builder().build());

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

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public List<Rule> getRules() {
		return rules;
	}

	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}

}
