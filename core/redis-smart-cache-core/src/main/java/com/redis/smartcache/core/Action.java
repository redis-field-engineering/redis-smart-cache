package com.redis.smartcache.core;

public class Action {

	private long ttl = RuleConfig.DEFAULT_TTL.toMillis();

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

}
