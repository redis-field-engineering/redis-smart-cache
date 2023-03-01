package com.redis.smartcache.core;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public interface Executable<T> extends Callable<T> {

	@Override
	T call() throws SQLException;

}