package com.redis.sidecar;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.sidecar.RulesConfig.RuleConfig;

class RulesConfigTests {

	@Test
	void propertyChangeListener() throws Exception {
		RulesConfig config = new RulesConfig();
		EventList eventList = new EventList();
		config.addPropertyChangeListener(eventList);
		RuleConfig newRule = RuleConfig.builder().tables(Arrays.asList("table1")).build();
		config.setRules(Arrays.asList(newRule));
		Assertions.assertEquals(1, eventList.getEvents().size());
		Assertions.assertEquals(RulesConfig.PROPERTY_RULES, eventList.getEvents().get(0).getPropertyName());
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
