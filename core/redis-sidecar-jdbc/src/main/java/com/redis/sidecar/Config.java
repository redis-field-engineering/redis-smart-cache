package com.redis.sidecar;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.internal.LettuceAssert;

public class Config {

	public static final Duration DEFAULT_REFRESH_RATE = Duration.ofSeconds(10);
	public static final Duration DEFAULT_METRICS_STEP = Duration.ofMinutes(1);

	private long refreshRate = DEFAULT_REFRESH_RATE.toSeconds();
	private Driver driver = new Driver();
	private Redis redis = new Redis();
	private Metrics metrics = new Metrics();
	private List<Rule> rules = Arrays.asList(Rule.builder().build());
	private int bufferSize = ResultSetCodec.DEFAULT_BYTE_BUFFER_CAPACITY;

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

	public Metrics getMetrics() {
		return metrics;
	}

	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

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

	public static class Driver {

		private String className;
		private String url;

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

	}

	public static class Redis {

		public static final String DEFAULT_URI = "redis://localhost:6379";
		public static final String DEFAULT_KEYSPACE = "sidecar";
		public static final String DEFAULT_KEY_SEPARATOR = ":";

		private String uri = DEFAULT_URI;
		private boolean tls;
		private boolean insecure;
		private boolean cluster;
		private String username;
		private String password;
		private String keyspace = DEFAULT_KEYSPACE;
		private String keySeparator = DEFAULT_KEY_SEPARATOR;
		private Pool pool = new Pool();

		public String getUri() {
			return uri;
		}

		public void setUri(String uri) {
			this.uri = uri;
		}

		public boolean isTls() {
			return tls;
		}

		public void setTls(boolean tls) {
			this.tls = tls;
		}

		public String key(String id) {
			return key(keyspace, id);
		}

		public String key(String keyspace, String id) {
			return keyspace + keySeparator + id;
		}

		public String getKeyspace() {
			return keyspace;
		}

		public void setKeyspace(String keyspace) {
			this.keyspace = keyspace;
		}

		public String getKeySeparator() {
			return keySeparator;
		}

		public void setKeySeparator(String keySeparator) {
			this.keySeparator = keySeparator;
		}

		public String getUsername() {
			return username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		public boolean isInsecure() {
			return insecure;
		}

		public void setInsecure(boolean insecure) {
			this.insecure = insecure;
		}

		public boolean isCluster() {
			return cluster;
		}

		public void setCluster(boolean cluster) {
			this.cluster = cluster;
		}

		public Pool getPool() {
			return pool;
		}

		public void setPool(Pool pool) {
			this.pool = pool;
		}

	}

	public static class Pool {

		public static final int DEFAULT_MAX_IDLE = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		public static final int DEFAULT_MIN_IDLE = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
		public static final int DEFAULT_MAX_TOTAL = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		public static final Duration DEFAULT_MAX_WAIT = BaseObjectPoolConfig.DEFAULT_MAX_WAIT;
		public static final Duration DEFAULT_TIME_BETWEEN_EVICTION_RUNS = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS;

		/**
		 * Maximum number of "idle" connections in the pool. Use a negative value to
		 * indicate an unlimited number of idle connections.
		 */
		private int maxIdle = DEFAULT_MAX_IDLE;

		/**
		 * Target for the minimum number of idle connections to maintain in the pool.
		 * This setting only has an effect if both it and time between eviction runs are
		 * positive.
		 */
		private int minIdle = DEFAULT_MIN_IDLE;

		/**
		 * Maximum number of connections that can be allocated by the pool at a given
		 * time. Use a negative value for no limit.
		 */
		private int maxActive = DEFAULT_MAX_TOTAL;

		/**
		 * Maximum amount of time a connection allocation should block before throwing
		 * an exception when the pool is exhausted. Use a negative value to block
		 * indefinitely.
		 */
		private long maxWait = DEFAULT_MAX_WAIT.toMillis();

		/**
		 * Time between runs of the idle object evictor thread. When positive, the idle
		 * object evictor thread starts, otherwise no idle object eviction is performed.
		 */
		private long timeBetweenEvictionRuns = DEFAULT_TIME_BETWEEN_EVICTION_RUNS.toMillis();

		public int getMaxIdle() {
			return this.maxIdle;
		}

		public void setMaxIdle(int maxIdle) {
			this.maxIdle = maxIdle;
		}

		public int getMinIdle() {
			return this.minIdle;
		}

		public void setMinIdle(int minIdle) {
			this.minIdle = minIdle;
		}

		public int getMaxActive() {
			return this.maxActive;
		}

		public void setMaxActive(int maxActive) {
			this.maxActive = maxActive;
		}

		public long getMaxWait() {
			return this.maxWait;
		}

		public void setMaxWait(long maxWait) {
			this.maxWait = maxWait;
		}

		public long getTimeBetweenEvictionRuns() {
			return this.timeBetweenEvictionRuns;
		}

		public void setTimeBetweenEvictionRuns(long timeBetweenEvictionRuns) {
			this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
		}

	}

	public static class Metrics {

		private long step = DEFAULT_METRICS_STEP.toSeconds();
		private boolean lettuce;

		public boolean isLettuce() {
			return lettuce;
		}

		public void setLettuce(boolean commandLatencyEnabled) {
			this.lettuce = commandLatencyEnabled;
		}

		public long getStep() {
			return step;
		}

		public void setStep(long step) {
			this.step = step;
		}

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
