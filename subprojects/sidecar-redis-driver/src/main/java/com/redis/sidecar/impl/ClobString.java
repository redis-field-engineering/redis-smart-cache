package com.redis.sidecar.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class ClobString implements Clob {

	private final String content;

	public ClobString(String content) {
		this.content = content;
	}

	@Override
	public long length() throws SQLException {
		return content == null ? 0 : content.length();
	}

	@Override
	public String getSubString(long pos, int length) throws SQLException {
		return content == null ? null : content.substring((int) pos - 1, length);
	}

	@Override
	public Reader getCharacterStream() throws SQLException {
		return content == null ? null : new StringReader(content);
	}

	@Override
	public InputStream getAsciiStream() throws SQLException {
		return content == null ? null : new ByteArrayInputStream(content.getBytes());
	}

	@Override
	public long position(String searchstr, long start) throws SQLException {
		if (content == null)
			return -1;
		final int pos = content.indexOf(searchstr, (int) start - 1);
		return pos == -1 ? pos : pos + 1;
	}

	@Override
	public long position(Clob searchstr, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int setString(long pos, String str) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int setString(long pos, String str, int offset, int len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public OutputStream setAsciiStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Writer setCharacterStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void truncate(long len) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void free() throws SQLException {
	}

	@Override
	public Reader getCharacterStream(long pos, long length) throws SQLException {
		return new StringReader(content.substring((int) pos - 1, (int) (pos + length - 1)));
	}
}
