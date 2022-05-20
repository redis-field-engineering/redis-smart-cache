package com.redis.sidecar.jdbc;

import java.util.Arrays;

import io.lettuce.core.internal.LettuceAssert;

public class RowId implements java.sql.RowId {

	private int hash;

	private final byte[] id;

	public RowId(byte[] id) {
		LettuceAssert.notNull(id, "id must not be null");
		this.id = id;
	}

	public RowId(RowId id) {
		this(id.getBytes());
	}

	public RowId(String hex) {
		LettuceAssert.notNull(hex, "hex must not be null");
		this.id = hexStringToByteArray(hex);
	}

	private static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	private static String byteArrayToHexString(byte[] bytes) {
		final char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
		char[] hexChars = new char[bytes.length * 2]; // Each byte has two hex characters (nibbles)
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF; // Cast bytes[j] to int, treating as unsigned value
			hexChars[j * 2] = hexArray[v >>> 4]; // Select hex character from upper nibble
			hexChars[j * 2 + 1] = hexArray[v & 0x0F]; // Select hex character from lower nibble
		}
		return new String(hexChars);
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof RowId) && Arrays.equals(this.id, ((RowId) obj).id);
	}

	@Override
	public byte[] getBytes() {
		return id.clone();
	}

	@Override
	public String toString() {
		return byteArrayToHexString(id);
	}

	@Override
	public int hashCode() {

		if (hash == 0) {
			hash = Arrays.hashCode(id);
		}

		return hash;
	}

}
