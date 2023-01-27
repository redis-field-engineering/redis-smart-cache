package com.redis.sidecar.codec;

import io.netty.buffer.ByteBuf;

public interface StringCodec {

	String decode(ByteBuf buffer);

	void encode(ByteBuf buffer, String string);

}