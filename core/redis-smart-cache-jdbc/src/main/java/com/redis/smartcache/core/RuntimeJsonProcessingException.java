package com.redis.smartcache.core;

import com.fasterxml.jackson.core.JsonProcessingException;

public class RuntimeJsonProcessingException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeJsonProcessingException(JsonProcessingException cause) {
		super(cause);
	}

	public RuntimeJsonProcessingException(String message) {
		super(message);
	}

	public RuntimeJsonProcessingException(String message, JsonProcessingException cause) {
		super(message, cause);
	}
}