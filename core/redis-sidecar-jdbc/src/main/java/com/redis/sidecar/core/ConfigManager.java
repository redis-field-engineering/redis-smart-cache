package com.redis.sidecar.core;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.json.SetMode;

import io.lettuce.core.AbstractRedisClient;

public class ConfigManager implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();

	private StatefulRedisModulesConnection<String, String> connection(AbstractRedisClient redisClient) {
		if (redisClient instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) redisClient).connect();
		}
		return ((RedisModulesClient) redisClient).connect();
	}

	public void register(AbstractRedisClient redisClient, String key, Object config, Duration refreshRate)
			throws JsonProcessingException {
		StatefulRedisModulesConnection<String, String> connection = connection(redisClient);
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		connection.sync().jsonSet(key, "$", json, SetMode.NX);
		ObjectReader reader = mapper.readerForUpdating(config);
		read(connection, key, reader);
		long refreshRateMillis = refreshRate.toMillis();
		futures.put(key, executor.scheduleAtFixedRate(() -> {
			try {
				read(connection, key, reader);
			} catch (JsonProcessingException e) {
				log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", key), e);
			}
		}, refreshRateMillis, refreshRateMillis, TimeUnit.MILLISECONDS));
	}

	private void read(StatefulRedisModulesConnection<String, String> connection, String key, ObjectReader reader)
			throws JsonProcessingException {
		String json = connection.sync().jsonGet(key);
		if (json == null || json.isEmpty()) {
			return;
		}
		reader.readValue(json);
	}

	public void stop() {
		close();
	}

	@Override
	public void close() {
		futures.forEach((k, v) -> v.cancel(false));
	}

}