package com.redis.smartcache.core;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.StringUtils;

public class KeyBuilder {

	public static final String EMPTY_STRING = "";
	public static final String DEFAULT_SEPARATOR = ":";

	private String prefix = EMPTY_STRING;
	private String separator = DEFAULT_SEPARATOR;
	private String keyspace = EMPTY_STRING;

	public String keyspace() {
		return keyspace;
	}

	public String prefix() {
		return prefix;
	}

	public String separator() {
		return separator;
	}

	public static KeyBuilder of(String keyspace) {
		return new KeyBuilder().withKeyspace(keyspace);
	}

	public KeyBuilder noKeyspace() {
		return withKeyspace(null);
	}

	public KeyBuilder withSeparator(String separator) {
		this.separator = separator;
		updatePrefix();
		return this;
	}

	public KeyBuilder withKeyspace(String keyspace) {
		this.keyspace = keyspace;
		updatePrefix();
		return this;
	}

	private void updatePrefix() {
		this.prefix = StringUtils.hasLength(keyspace) ? keyspace + separator : EMPTY_STRING;
	}

	public String build(Iterable<String> ids) {
		return prefix + String.join(separator, ids);
	}

	public String build(Object... ids) {
		return build(Stream.of(ids).filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toList()));
	}

	public String build(String... ids) {
		return build(Arrays.asList(ids));
	}

	/**
	 * Creates a KeyBuilder for keys under the given sub-id string. For example if
	 * this KeyBuilder is in keyspace "root" and the given id is "sub" then the
	 * returned KeyBuilder will create keys under "root:sub:"
	 * 
	 * @param keyspace sub-keyspace element
	 * @return KeyBuilder for sub-keyspace "id"
	 */
	public KeyBuilder sub(String keyspace) {
		return new KeyBuilder().withKeyspace(build(keyspace)).withSeparator(separator);
	}

}
