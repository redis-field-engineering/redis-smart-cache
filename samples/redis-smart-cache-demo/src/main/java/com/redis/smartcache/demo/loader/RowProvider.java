package com.redis.smartcache.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.redis.smartcache.demo.DemoConfig.DataConfig;

public interface RowProvider {

	void set(PreparedStatement statement, DataConfig config, int index) throws SQLException;

}