package com.redis.smartcache;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.core.EvictingLinkedHashMap;

class UtilTests {

	@Test
	void evictingLinkedHashMap() {
		int capacity = 10;
		Map<Integer, Integer> map = new EvictingLinkedHashMap<>(capacity);
		for (int index = 0; index < capacity; index++) {
			map.put(index, index);
		}
		for (int index = 5; index < capacity; index++) {
			map.get(index);
		}
		for (int index = capacity; index < capacity + 5; index++) {
			map.put(index, index);
		}
		Assertions.assertEquals(10, map.size());
		for (int index = 0; index < map.size(); index++) {
			Assertions.assertEquals(index >= 5, map.containsKey(index));
		}
	}
}
