package com.redis.smartcache.test;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnUpdater {

    void update(ResultSet rowSet, int columnIndex) throws SQLException;

}
