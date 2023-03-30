package com.redis.smartcache.core;

import java.util.Arrays;

public class KeyBuilder {

	public static final String DEFAULT_SEPARATOR = ":";

	private final String keyspace;
	private final String separator;

	public KeyBuilder(String keyspace) {
		this(keyspace, DEFAULT_SEPARATOR);
	}

	public KeyBuilder(String keyspace, String separator) {
		if (keyspace == null || keyspace.isEmpty()) {
			throw new IllegalArgumentException("Keyspace must not be null or empty");
		}
		this.keyspace = keyspace;
		this.separator = separator;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getSeparator() {
		return separator;
	}

	public String create(Iterable<String> ids) {
		StringBuilder builder = new StringBuilder(keyspace);
		for (String id : ids) {
			builder.append(separator).append(id);
		}
		return builder.toString();
	}

	public String create(String... ids) {
		return create(Arrays.asList(ids));
	}

	/**
	 * Creates a KeyBuilder for keys under the given sub-id string. For example if
	 * this KeyBuilder is in keyspace "root" and the given id is "sub" then the
	 * returned KeyBuilder will create keys under "root:sub:"
	 * 
	 * @param id subkeyspace element
	 * @return KeyBuilder for sub-keyspace "id"
	 */
	public KeyBuilder builder(String id) {
		return new KeyBuilder(create(id), separator);
	}

}
