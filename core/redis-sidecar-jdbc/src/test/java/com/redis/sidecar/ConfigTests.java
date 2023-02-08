package com.redis.sidecar;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.sidecar.RulesetConfig.RuleConfig;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

class ConfigTests {

	private static final String PROPERTY_CODEC_BUFFER_SIZE = "sidecar.redis.codec-buffer-size";

	@Test
	void bootstrapProperties() throws IOException {
		BootstrapConfig config = new BootstrapConfig();
		DataSize bufferSize = DataSize.of(123, Unit.KILOBYTE);
		config.getRedis().setCodecBufferSize(bufferSize);
		PropsMapper mapper = new PropsMapper();
		Properties properties = mapper.write(config);
		Assertions.assertEquals(bufferSize, DataSize.valueOf(properties.getProperty(PROPERTY_CODEC_BUFFER_SIZE)));
		BootstrapConfig actual = mapper.read(properties, BootstrapConfig.class);
		Assertions.assertEquals(config, actual);
		properties.setProperty(PROPERTY_CODEC_BUFFER_SIZE, "10MB");
		Assertions.assertEquals(DataSize.of(10, Unit.MEGABYTE),
				mapper.read(properties, BootstrapConfig.class).getRedis().getCodecBufferSize());
	}

	@Test
	void ruleConfigChange() throws Exception {
		RulesetConfig config = new RulesetConfig();
		EventList eventList = new EventList();
		config.addPropertyChangeListener(eventList);
		RuleConfig newRule = RuleConfig.tables("table1").build();
		config.setRules(Arrays.asList(newRule));
		Assertions.assertEquals(1, eventList.getEvents().size());
		Assertions.assertEquals(RulesetConfig.PROPERTY_RULESET, eventList.getEvents().get(0).getPropertyName());
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
