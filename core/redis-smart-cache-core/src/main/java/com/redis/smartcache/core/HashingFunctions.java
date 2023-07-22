package com.redis.smartcache.core;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public class HashingFunctions {

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private HashingFunctions() {
    }

    public static long crc32(String string) {
        CRC32 crc = new CRC32();
        crc.update(string.getBytes(CHARSET));
        return crc.getValue();
    }

}
