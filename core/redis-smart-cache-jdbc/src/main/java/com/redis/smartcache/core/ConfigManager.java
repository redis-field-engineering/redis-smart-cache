package com.redis.smartcache.core;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface ConfigManager<T> extends AutoCloseable {

	/**
	 * 
	 * @return the config object, or null if none
	 */
	T get();

	void start() throws IOException;

	@Override
	default void close() throws Exception {
		stop();
	}

	void stop() throws InterruptedException, ExecutionException, TimeoutException;

}
