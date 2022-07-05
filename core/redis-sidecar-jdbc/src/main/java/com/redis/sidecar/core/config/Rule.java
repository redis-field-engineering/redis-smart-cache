package com.redis.sidecar.core.config;

import java.time.Duration;

import io.lettuce.core.internal.LettuceAssert;

public class Rule {

	public static final long TTL_NO_CACHE = 0;
	public static final long TTL_NO_EXPIRATION = -1;
	public static final Duration DEFAULT_TTL = Duration.ofHours(1);

	private String table;
	private long ttl = DEFAULT_TTL.toSeconds();

	public Rule() {
	}

	private Rule(Builder builder) {
		this.table = builder.table;
		this.ttl = builder.ttl.toSeconds();
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	/**
	 * 
	 * @return Key expiration duration in seconds. Use 0 for no caching, -1 for no
	 *         expiration
	 */
	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	@Override
	public String toString() {
		return "Rule [table=" + table + ", ttl=" + ttl + "]";
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String table;
		private Duration ttl = DEFAULT_TTL;

		private Builder() {
		}

		public Builder table(String table) {
			this.table = table;
			return this;
		}

		public Builder ttl(Duration ttl) {
			LettuceAssert.notNull(ttl, "TTL must not be null");
			LettuceAssert.isTrue(!ttl.isNegative(), "TTL must be positive");
			this.ttl = ttl;
			return this;
		}

		public Rule build() {
			return new Rule(this);
		}
	}

}