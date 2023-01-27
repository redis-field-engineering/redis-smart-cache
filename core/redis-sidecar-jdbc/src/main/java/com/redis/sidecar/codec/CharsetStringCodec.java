package com.redis.sidecar.codec;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class CharsetStringCodec implements StringCodec {

	public static final StringCodec UTF8 = CharsetStringCodec.of(StandardCharsets.UTF_8);

	private final Charset charset;

	public CharsetStringCodec(Charset charset) {
		this.charset = charset;
	}

	@Override
	public String decode(ByteBuf buffer) {
		int length = buffer.readInt();
		byte[] bytes = new byte[length];
		buffer.readBytes(bytes);
		return new String(bytes, charset);
	}

	@Override
	public void encode(ByteBuf buffer, String string) {
		byte[] bytes = string.getBytes(charset);
		buffer.writeInt(bytes.length);
		buffer.writeBytes(bytes);
	}

	public static CharsetStringCodec of(Charset charset) {
		return new CharsetStringCodec(charset);
	}
}