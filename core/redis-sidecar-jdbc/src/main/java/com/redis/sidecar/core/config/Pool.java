package com.redis.sidecar.core.config;

import java.time.Duration;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Pool {

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