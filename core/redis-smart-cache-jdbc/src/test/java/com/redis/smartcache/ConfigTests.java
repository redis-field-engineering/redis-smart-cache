package com.redis.smartcache;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.Config.RulesetConfig.RuleConfig;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

class ConfigTests {

	@Test
	void keyBuilder() {
		Config config = new Config();
		Assertions.assertEquals(Config.DEFAULT_KEYSPACE + Config.DEFAULT_KEY_SEPARATOR + Driver.CACHE_KEY_PREFIX
				+ Config.DEFAULT_KEY_SEPARATOR, config.key(Driver.CACHE_KEY_PREFIX, ""));
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
