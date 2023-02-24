package com.redis.smartcache.core.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public class CRC32HashingFunction implements HashingFunction {

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	@Override
	public String hash(String string) {
		CRC32 crc = new CRC32();
		crc.update(string.getBytes(CHARSET));
		return String.valueOf(crc.getValue());
	}

}
