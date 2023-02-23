package com.redis.smartcache.core.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class EvictingLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

	public enum OrderingMode {
		ACCESS, INSERT
	}

	private static final long serialVersionUID = -7701362904798769327L;

	public static final float DEFAULT_LOAD_FACTOR = 0.75f;
	public static final OrderingMode DEFAULT_ORDERING_MODE = OrderingMode.ACCESS;

	private final int capacity;

	public EvictingLinkedHashMap(int capacity) {
		this(capacity, DEFAULT_LOAD_FACTOR, DEFAULT_ORDERING_MODE);
	}

	/**
	 * Constructs an empty {@code LinkedHashMap} instance with the specified initial
	 * capacity, load factor and ordering mode.
	 *
	 * @param initialCapacity the initial capacity
	 * @param loadFactor      the load factor
	 * @param orderingMode     the ordering mode
	 * @throws IllegalArgumentException if the initial capacity is negative or the
	 *                                  load factor is nonpositive
	 */
	public EvictingLinkedHashMap(int initialCapacity, float loadFactor, OrderingMode orderingMode) {
		super(initialCapacity, loadFactor, orderingMode == OrderingMode.ACCESS);
		this.capacity = initialCapacity;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > capacity;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(capacity);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EvictingLinkedHashMap<?, ?> other = (EvictingLinkedHashMap<?, ?>) obj;
		return capacity == other.capacity;
	}
}