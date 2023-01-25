package com.redis.sidecar.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Date;

import com.redis.sidecar.demo.Config.Loader;

public interface RowProvider {

	void set(PreparedStatement statement, Loader config, int index) throws SQLException;

	static java.sql.Date sqlDate(Date date) {
		if (date == null) {
			return null;
		}
		return java.sql.Date.valueOf(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
	}

}