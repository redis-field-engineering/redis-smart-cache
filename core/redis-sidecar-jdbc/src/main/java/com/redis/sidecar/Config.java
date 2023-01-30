package com.redis.sidecar;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import com.redis.sidecar.codec.ResultSetCodec;

import io.lettuce.core.internal.LettuceAssert;

public class Config {

	private List<Rule> rules = Arrays.asList(Rule.builder().build());
	private int bufferSize = ResultSetCodec.DEFAULT_BYTE_BUFFER_CAPACITY;

	public List<Rule> getRules() {
		return rules;
	}

	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}

	/**
	 * 
	 * @return max byte buffer capacity in bytes
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public static class Rule {

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
				LettuceAssert.isTrue(!ttl.isNegative(), "TTL must be zero or greater");
				this.ttl = ttl;
				return this;
			}

			public Rule build() {
				return new Rule(this);
			}
		}

	}

}
