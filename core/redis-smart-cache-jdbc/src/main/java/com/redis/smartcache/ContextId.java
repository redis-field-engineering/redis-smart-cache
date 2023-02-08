package com.redis.smartcache;

import java.util.Objects;

public class ContextId {

	private final String url;
	private final String name;

	public ContextId(String url, String name) {
		this.url = url;
		this.name = name;
	}

	public String getUrl() {
		return url;
	}

	public String getName() {
		return name;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, url);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ContextId other = (ContextId) obj;
		return Objects.equals(name, other.name) && Objects.equals(url, other.url);
	}

	public static ContextId of(String url, String name) {
		return new ContextId(url, name);
	}

}
