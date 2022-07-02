package com.redis.sidecar.config;

public class ByteSize {

	private static final int KB = 1024;
	private static final int MB = KB * KB;

	private final int bytes;

	private ByteSize(int bytes) {
		this.bytes = bytes;
	}

	public int toBytes() {
		return bytes;
	}

	public static ByteSize ofMB(int number) {
		return new ByteSize(number * MB);
	}

	public static ByteSize ofKB(int number) {
		return new ByteSize(number * KB);
	}
}