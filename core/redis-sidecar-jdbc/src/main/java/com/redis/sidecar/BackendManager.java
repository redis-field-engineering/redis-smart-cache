package com.redis.sidecar;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BackendManager {

	private Map<String, Driver> drivers = new HashMap<>();

	public Connection connect(Config config, Properties info) throws SQLException {
		String className = config.getDriver().getClassName();
		if (className == null || className.isEmpty()) {
			throw new SQLException("No backend driver class specified");
		}
		String url = config.getDriver().getUrl();
		if (url == null || url.isEmpty()) {
			throw new SQLException("No backend URL specified");
		}
		Driver driver;
		if (drivers.containsKey(className)) {
			driver = drivers.get(className);
		} else {
			try {
				driver = (Driver) Class.forName(className).getConstructor().newInstance();
			} catch (Exception e) {
				throw new SQLException("Cannot initialize backend driver '" + className + "'", e);
			}
			drivers.put(className, driver);
		}
		return driver.connect(url, info);
	}

}
