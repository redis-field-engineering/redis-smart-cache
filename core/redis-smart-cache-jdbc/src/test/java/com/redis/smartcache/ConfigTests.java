package com.redis.smartcache;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.Config.RulesetConfig.RuleConfig;
import com.redis.smartcache.core.config.ConfigManager;
import com.redis.smartcache.core.config.ObjectMappers;
import com.redis.testcontainers.RedisStackContainer;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

@Testcontainers
class ConfigTests {

	@Container
	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Test
	void keyBuilder() {
		Config config = new Config();
		Assertions.assertEquals(Config.DEFAULT_KEYSPACE + Config.DEFAULT_KEY_SEPARATOR + Driver.CACHE_KEY_PREFIX
				+ Config.DEFAULT_KEY_SEPARATOR, config.key(Driver.CACHE_KEY_PREFIX, ""));
	}

	@Test
	void jsonMapper() throws JsonProcessingException {
		Config config = new Config();
		String json = ObjectMappers.json().writeValueAsString(config);
		System.out.println(json);
	}

	@Test
	void updateConfig() throws Exception {
		String key = "testUpdate";
		RulesetConfig config = new RulesetConfig();
		Duration interval = Duration.ofMillis(100);
		RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
		StatefulRedisModulesConnection<String, String> connection = client.connect();
		try (ConfigManager<RulesetConfig> manager = new ConfigManager<>(ObjectMappers.json(), connection, key, config,
				interval)) {
			manager.start();
			Assertions.assertNotNull(connection.sync().jsonGet(key));
			long ttl = 123;
			connection.sync().jsonSet(key, ".rules[0].ttl", MessageFormat.format("\"{0}s\"", ttl));
			Awaitility.await().timeout(Duration.ofMillis(300)).until(() -> config.getRules().size() == 1
					&& config.getRules().get(0).getTtl().getValue(TimeUnit.SECONDS) == ttl);
		}
	}

	@Test
	void configProperties() throws IOException {
		String propertyName = Driver.PROPERTY_PREFIX + ".codec-buffer-size";
		Config config = new Config();
		DataSize bufferSize = DataSize.of(123, Unit.KILOBYTE);
		config.setCodecBufferSize(bufferSize);
		Properties properties = Driver.properties(config);
		Assertions.assertEquals(bufferSize, DataSize.valueOf(properties.getProperty(propertyName)));
		Config actual = Driver.config(properties);
		Assertions.assertEquals(config, actual);
		properties.setProperty(propertyName, "10MB");
		Assertions.assertEquals(DataSize.of(10, Unit.MEGABYTE), Driver.config(properties).getCodecBufferSize());
	}

	@Test
	void ruleConfigChange() throws Exception {
		RulesetConfig config = new RulesetConfig();
		EventList eventList = new EventList();
		config.addPropertyChangeListener(eventList);
		RuleConfig newRule = RuleConfig.tables("table1").build();
		config.setRules(Arrays.asList(newRule));
		Assertions.assertEquals(1, eventList.getEvents().size());
		Assertions.assertEquals(RulesetConfig.PROPERTY_RULES, eventList.getEvents().get(0).getPropertyName());
	}

	class EventList implements PropertyChangeListener {

		private final List<PropertyChangeEvent> events = new ArrayList<>();

		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			events.add(evt);
		}

		public List<PropertyChangeEvent> getEvents() {
			return events;
		}
	}

}
