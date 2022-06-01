package com.redis.sidecar.core;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Config {

	public static final String DEFAULT_KEYSPACE = "sidecar";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final ByteSize DEFAULT_BUFFER_SIZE = ByteSize.ofMB(100);
	public static final String DEFAULT_CACHE_NAME = "default";
	public static final Duration DEFAULT_REFRESH_RATE = Duration.ofSeconds(10);
	public static final Duration DEFAULT_METRICS_PUBLISH_INTERVAL = Duration.ofMinutes(1);
	public static final int DEFAULT_POOL_MAX_IDLE = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	public static final int DEFAULT_POOL_MIN_IDLE = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	public static final int DEFAULT_POOL_MAX_TOTAL = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	public static final Duration DEFAULT_POOL_MAX_WAIT = GenericObjectPoolConfig.DEFAULT_MAX_WAIT;
	public static final Duration DEFAULT_POOL_TIME_BETWEEN_EVICTION_RUNS = GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
	public static final Duration DEFAULT_TTL = Duration.ofHours(1);
	public static final long TTL_NO_CACHE = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private String keyspace = DEFAULT_KEYSPACE;
	private String keySeparator = DEFAULT_KEY_SEPARATOR;
	private String cacheName = DEFAULT_CACHE_NAME;
	private int bufferSize = DEFAULT_BUFFER_SIZE.toBytes();
	private long refreshRate = DEFAULT_REFRESH_RATE.toSeconds();
	private Redis redis = new Redis();
	private Driver driver = new Driver();
	private Metrics metrics = new Metrics();
	private List<Rule> rules = Arrays.asList(Rule.builder().ttl(DEFAULT_TTL.toSeconds()).build());

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String key(String... ids) {
		StringJoiner joiner = new StringJoiner(keySeparator);
		joiner.add(keyspace);
		joiner.add(cacheName);
		for (String id : ids) {
			joiner.add(id);
		}
		return joiner.toString();
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	public long getRefreshRate() {
		return refreshRate;
	}

	public void setRefreshRate(long refreshRate) {
		this.refreshRate = refreshRate;
	}

	public Redis getRedis() {
		return redis;
	}

	public void setRedis(Redis redis) {
		this.redis = redis;
	}

	public Driver getDriver() {
		return driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
	}

	public Metrics getMetrics() {
		return metrics;
	}

	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
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

	public static class Rule {

		private String table;
		private long ttl = DEFAULT_TTL.toSeconds();

		public Rule() {
		}

		private Rule(Builder builder) {
			this.table = builder.table;
			this.ttl = builder.ttl;
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
			private long ttl = DEFAULT_TTL.toSeconds();

			private Builder() {
			}

			public Builder table(String table) {
				this.table = table;
				return this;
			}

			public Builder ttl(long ttl) {
				this.ttl = ttl;
				return this;
			}

			public Rule build() {
				return new Rule(this);
			}
		}

	}

	public static class ByteSize {

		private static final int KB = 1024;
		private static final int MB = KB * KB;

		private final int bytes;

		private ByteSize(int bytes) {
			this.bytes = bytes;
		}

		public int toBytes() {
			return bytes;
		}

		public static ByteSize ofMB(int number) {
			return new ByteSize(number * MB);
		}

		public static ByteSize ofKB(int number) {
			return new ByteSize(number * KB);
		}
	}

	public static class Metrics {

		private long publishInterval = DEFAULT_METRICS_PUBLISH_INTERVAL.toSeconds();

		public long getPublishInterval() {
			return publishInterval;
		}

		public void setPublishInterval(long seconds) {
			this.publishInterval = seconds;
		}
	}

	public static class Redis {

		private String uri;
		private boolean cluster;
		private Pool pool = new Pool();

		public String getUri() {
			return uri;
		}

		public void setUri(String uri) {
			this.uri = uri;
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

		public static class Pool {

			/**
			 * Maximum number of "idle" connections in the pool. Use a negative value to
			 * indicate an unlimited number of idle connections.
			 */
			private int maxIdle = DEFAULT_POOL_MAX_IDLE;

			/**
			 * Target for the minimum number of idle connections to maintain in the pool.
			 * This setting only has an effect if both it and time between eviction runs are
			 * positive.
			 */
			private int minIdle = DEFAULT_POOL_MIN_IDLE;

			/**
			 * Maximum number of connections that can be allocated by the pool at a given
			 * time. Use a negative value for no limit.
			 */
			private int maxActive = DEFAULT_POOL_MAX_TOTAL;

			/**
			 * Maximum amount of time a connection allocation should block before throwing
			 * an exception when the pool is exhausted. Use a negative value to block
			 * indefinitely.
			 */
			private long maxWait = DEFAULT_POOL_MAX_WAIT.toMillis();

			/**
			 * Time between runs of the idle object evictor thread. When positive, the idle
			 * object evictor thread starts, otherwise no idle object eviction is performed.
			 */
			private long timeBetweenEvictionRuns = DEFAULT_POOL_TIME_BETWEEN_EVICTION_RUNS.toMillis();

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

}
