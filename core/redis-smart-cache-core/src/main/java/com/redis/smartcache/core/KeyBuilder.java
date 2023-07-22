package com.redis.smartcache.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.smartcache.core.config.Config;

public class KeyBuilder {

	public static final String DEFAULT_SEPARATOR = ":";

	private String separator = DEFAULT_SEPARATOR;
	private Optional<String> keyspace = Optional.empty();

	public Optional<String> keyspace() {
		return keyspace;
	}

	public String separator() {
		return separator;
	}

	public static KeyBuilder create() {
		return new KeyBuilder();
	}

	public static KeyBuilder of(String keyspace) {
		return new KeyBuilder().withKeyspace(keyspace);
	}

	public KeyBuilder noKeyspace() {
		this.keyspace = Optional.empty();
		return this;
	}

	public KeyBuilder withSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	public KeyBuilder withKeyspace(String keyspace) {
		this.keyspace = Optional.of(keyspace);
		return this;
	}

	public String build(Iterable<String> ids) {
		List<String> items = new ArrayList<>();
		keyspace.ifPresent(items::add);
		ids.forEach(items::add);
		return join(items);
	}

	public String build(Object... ids) {
		return build(toString(ids));
	}

	private Iterable<String> toString(Object... ids) {
		return Stream.of(ids).filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toList());
	}

	public String build(String... ids) {
		return build(Arrays.asList(ids));
	}

	public String join(Iterable<String> ids) {
		return String.join(separator, ids);
	}

	public String join(String... ids) {
		return join(Arrays.asList(ids));
	}

	public String join(Object... ids) {
		return join(toString(ids));
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

	public static KeyBuilder of(Config config) {
		return KeyBuilder.of(config.getName()).withSeparator(config.getCache().getKeySeparator());
	}

}
