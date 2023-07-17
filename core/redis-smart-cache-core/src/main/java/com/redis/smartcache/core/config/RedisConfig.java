package com.redis.smartcache.core.config;

import java.util.Objects;

import com.redis.smartcache.core.KeyBuilder;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.lettuce.core.SslVerifyMode;

public class RedisConfig {

	public static final DataSize DEFAULT_BUFFER_CAPACITY = DataSize.of(10, Unit.MEGABYTE);

	private String uri;
	private boolean cluster;
	private boolean tls;
	private SslVerifyMode tlsVerify = SslVerifyMode.NONE;
	private String username;
	private char[] password;
	private DataSize codecBufferCapacity = DEFAULT_BUFFER_CAPACITY;
	private String keySeparator = KeyBuilder.DEFAULT_SEPARATOR;

	/**
	 * 
	 * @return max byte buffer capacity in bytes
	 */
	public DataSize getCodecBufferCapacity() {
		return codecBufferCapacity;
	}

	public void setCodecBufferCapacity(DataSize size) {
		this.codecBufferCapacity = size;
	}

	public void setCodecBufferSizeInBytes(long size) {
		this.codecBufferCapacity = DataSize.ofBytes(size);
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public SslVerifyMode getTlsVerify() {
		return tlsVerify;
	}

	public void setTlsVerify(SslVerifyMode tlsVerify) {
		this.tlsVerify = tlsVerify;
	}

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

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	@Override
	public int hashCode() {
		return Objects.hash(uri);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisConfig other = (RedisConfig) obj;
		return Objects.equals(uri, other.uri);
	}

}