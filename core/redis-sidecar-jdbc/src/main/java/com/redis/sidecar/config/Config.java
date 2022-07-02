package com.redis.sidecar.config;

import java.util.Arrays;
import java.util.List;

public class Config {

	public static final ByteSize DEFAULT_BUFFER_SIZE = ByteSize.ofMB(100);
	public static final long TTL_NO_CACHE = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private int bufferSize = DEFAULT_BUFFER_SIZE.toBytes();
	private List<Rule> rules = Arrays.asList(Rule.builder().build());
	private Pool pool = new Pool();

	public Pool getPool() {
		return pool;
	}

	public void setPool(Pool pool) {
		this.pool = pool;
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
