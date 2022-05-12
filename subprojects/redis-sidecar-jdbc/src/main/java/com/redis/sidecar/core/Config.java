package com.redis.sidecar.core;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Config {

	private static final int DEFAULT_BUFFER_SIZE = 100 * 1024 * 1024;
	public static final String DEFAULT_KEY = "com.redis.sidecar";

	private String key = DEFAULT_KEY;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private long refreshRate = Duration.ofSeconds(10).toMillis();
	private Redis redis = new Redis();
	private Driver driver = new Driver();

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
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

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
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
			private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;

			/**
			 * Target for the minimum number of idle connections to maintain in the pool.
			 * This setting only has an effect if both it and time between eviction runs are
			 * positive.
			 */
			private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

			/**
			 * Maximum number of connections that can be allocated by the pool at a given
			 * time. Use a negative value for no limit.
			 */
			private int maxActive = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

			/**
			 * Maximum amount of time a connection allocation should block before throwing
			 * an exception when the pool is exhausted. Use a negative value to block
			 * indefinitely.
			 */
			private long maxWait = GenericObjectPoolConfig.DEFAULT_MAX_WAIT.toMillis();

			/**
			 * Time between runs of the idle object evictor thread. When positive, the idle
			 * object evictor thread starts, otherwise no idle object eviction is performed.
			 */
			private long timeBetweenEvictionRuns = GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS
					.toMillis();

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
