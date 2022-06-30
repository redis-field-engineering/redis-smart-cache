package com.redis.sidecar.core;

import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.sidecar.SidecarDriver;

class ConfigTests {

	@Test
	void testConfigDeserialization() throws IOException {
		String className = SidecarDriver.class.getName();
		String url = "jdbc:redis://localhost:6379";
		Properties info = new Properties();
		info.setProperty("sidecar.driver.class-name", className);
		info.setProperty("sidecar.driver.url", url);
		Config config = SidecarDriver.config(info);
		Assertions.assertEquals(className, config.getDriver().getClassName());
		Assertions.assertEquals(url, config.getDriver().getUrl());
	}

}
