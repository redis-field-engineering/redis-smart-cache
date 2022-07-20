package com.redis.sidecar;

import java.sql.DriverManager;
import java.sql.SQLException;

public class SidecarDriver extends NonRegisteringSidecarDriver {

	static {
		try {
			DriverManager.registerDriver(new SidecarDriver());
		} catch (SQLException e) {
			throw new RuntimeException("Can't register driver");
		}
	}

	public SidecarDriver() {
		// Required for Class.forName().newInstance()
	}

}
