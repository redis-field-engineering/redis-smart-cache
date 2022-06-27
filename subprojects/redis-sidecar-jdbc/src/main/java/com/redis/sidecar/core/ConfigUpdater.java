package com.redis.sidecar.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.json.SetMode;

public class ConfigUpdater implements Runnable, AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigUpdater.class.getName());

	private final ObjectMapper mapper = new ObjectMapper();
	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final StatefulRedisModulesConnection<String, String> connection;
	private final Config config;
	private final ObjectWriter writer;
	private final ObjectReader reader;
	private final ScheduledFuture<?> future;

	public ConfigUpdater(StatefulRedisModulesConnection<String, String> connection, Config config)
			throws JsonProcessingException {
		this.connection = connection;
		this.config = config;
		this.writer = mapper.writerFor(config.getClass());
		this.reader = mapper.readerForUpdating(config);
		connection.sync().jsonSet(config.configKey(), "$", writer.writeValueAsString(config), SetMode.NX);
		this.future = executor.scheduleAtFixedRate(this, 0, config.getRefreshRate(), TimeUnit.SECONDS);
	}

	@Override
	public void run() {
		String json = connection.sync().jsonGet(config.configKey());
		if (json == null || json.isEmpty()) {
			return;
		}
		try {
			reader.readValue(json);
		} catch (JsonProcessingException e) {
			log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", config.configKey()), e);
		}
	}

	@Override
	public void close() {
		future.cancel(false);
		connection.close();
	}

}