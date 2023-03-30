package com.redis.smartcache.core;

public class RuntimeSQLException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeSQLException() {
		super();
	}

	public RuntimeSQLException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RuntimeSQLException(String message, Throwable cause) {
		super(message, cause);
	}

	public RuntimeSQLException(String message) {
		super(message);
	}

	public RuntimeSQLException(Throwable cause) {
		super(cause);
	}

}
