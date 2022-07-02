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
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.json.SetMode;

public class ConfigUpdater implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigUpdater.class.getName());

	public static final Duration DEFAULT_REFRESH_RATE = Duration.ofSeconds(10);

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final StatefulRedisModulesConnection<String, String> connection;
	private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();

	public ConfigUpdater(StatefulRedisModulesConnection<String, String> connection) {
		this.connection = connection;
	}

	public void create(String key, Object config) throws JsonProcessingException {
		create(key, config, DEFAULT_REFRESH_RATE);
	}

	public void create(String key, Object config, Duration refreshRate) throws JsonProcessingException {
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		connection.sync().jsonSet(key, "$", json, SetMode.NX);
		ObjectReader reader = mapper.readerForUpdating(config);
		read(key, reader);
		futures.put(key, executor.scheduleAtFixedRate(() -> {
			try {
				read(key, reader);
			} catch (JsonProcessingException e) {
				log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", key), e);
			}
		}, refreshRate.toMillis(), refreshRate.toMillis(), TimeUnit.MILLISECONDS));
	}

	private void read(String key, ObjectReader reader) throws JsonProcessingException {
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
		connection.close();
	}

}