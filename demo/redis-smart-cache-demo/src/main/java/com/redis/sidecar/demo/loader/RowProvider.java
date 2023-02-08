package com.redis.sidecar.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.redis.sidecar.demo.SidecarDemoConfig.DemoConfig;

public interface RowProvider {

	void set(PreparedStatement statement, DemoConfig config, int index) throws SQLException;

}