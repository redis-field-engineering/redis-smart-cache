package com.redis.sidecar.core.config;

import io.lettuce.core.RedisURI;

public class Redis {

	public static final String DEFAULT_URI = "redis://localhost:6379";
	public static final String DEFAULT_KEYSPACE = "sidecar";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final int DEFAULT_BUFFER_SIZE = 100; // MB

	private String uri = DEFAULT_URI;
	private boolean tls;
	private boolean insecure;
	private boolean cluster;
	private String username;
	private String password;
	private String keyspace = DEFAULT_KEYSPACE;
	private String keySeparator = DEFAULT_KEY_SEPARATOR;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
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

	public RedisURI redisURI() {
		RedisURI redisURI = RedisURI.create(uri);
		redisURI.setVerifyPeer(!insecure);
		if (tls) {
			redisURI.setSsl(tls);
		}
		if (username != null) {
			redisURI.setUsername(username);
		}
		if (password != null) {
			redisURI.setPassword((CharSequence) password);
		}
		return redisURI;
	}

	/**
	 * 
	 * @return max byte buffer capacity in megabytes
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
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
