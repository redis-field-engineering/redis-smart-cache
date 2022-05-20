package com.redis.sidecar.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Clob implements java.sql.Clob {

	private static final Logger log = Logger.getLogger(Clob.class.getName());

	@Override
	public long length() throws SQLException {
		return getData().length();
	}

	@Override
	public String getSubString(final long pos, final int length) throws SQLException {

		final String data = getData();
		final int dlen = data.length();

		if (pos == MIN_POS && length == dlen) {
			return data;
		}

		if (pos < MIN_POS || pos > dlen) {
			throw outOfRangeArgument("pos: " + pos);
		}

		final long index = pos - 1;

		if (length < 0 || length > dlen - index) {
			throw outOfRangeArgument("length: " + length);
		}

		return data.substring((int) index, (int) index + length);
	}

	private static SQLException outOfRangeArgument(String message) {
		return new SQLException("Out of range argument " + message);
	}

	@Override
	public Reader getCharacterStream() throws SQLException {
		return new StringReader(getData());
	}

	@Override
	public InputStream getAsciiStream() throws SQLException {

		try {
			return new ByteArrayInputStream(getData().getBytes("US-ASCII"));
		} catch (UnsupportedEncodingException e) {
			log.log(Level.WARNING, e.getMessage(), e);
			return null;
		}
	}

	@Override
	public long position(final String searchstr, long start) throws SQLException {

		final String data = getData();

		if (start < MIN_POS) {
			throw outOfRangeArgument("start: " + start);
		}

		if (searchstr == null || start > MAX_POS) {
			return -1;
		}

		final int position = data.indexOf(searchstr, (int) start - 1);

		return (position == -1) ? -1 : position + 1;
	}

	@Override
	public long position(final java.sql.Clob searchstr, final long start) throws SQLException {

		final String data = getData();

		if (start < MIN_POS) {
			throw outOfRangeArgument("start: " + start);
		}

		if (searchstr == null) {
			return -1;
		}

		final long dlen = data.length();
		final long sslen = searchstr.length();
		final long startIndex = start - 1;

		// This is potentially much less expensive than materializing a large
		// substring from some other vendor's CLOB. Indeed, we should probably
		// do the comparison piecewise, using an in-memory buffer (or temp-files
		// when available), if it is detected that the input CLOB is very long.
		if (startIndex > dlen - sslen) {
			return -1;
		}

		// by now, we know sslen and startIndex are both < Integer.MAX_VALUE
		String pattern;

		if (searchstr instanceof Clob) {
			pattern = ((Clob) searchstr).getData();
		} else {
			pattern = searchstr.getSubString(1L, (int) sslen);
		}

		final int index = data.indexOf(pattern, (int) startIndex);

		return (index == -1) ? -1 : index + 1;
	}

	@Override
	public int setString(long pos, String str) throws SQLException {
		return setString(pos, str, 0, str == null ? 0 : str.length());
	}

	private static SQLException nullArgument(String arg) {
		return new SQLException("Null argument: " + arg);
	}

	@Override
	public int setString(final long pos, final String str, final int offset, final int len) throws SQLException {
		checkReadonly();

		final String data = getData();

		if (str == null) {
			throw nullArgument("str");
		}

		final int strlen = str.length();
		final int dlen = data.length();
		final int ipos = (int) (pos - 1);

		if (offset == 0 && len == strlen && ipos == 0 && len >= dlen) {
			setData(str);
			return len;
		}

		if (offset < 0 || offset > strlen) {
			throw outOfRangeArgument("offset: " + offset);
		}

		if (len < 0 || len > strlen - offset) {
			throw outOfRangeArgument("len: " + len);
		}

		if (pos < MIN_POS || (pos - MIN_POS) > (Integer.MAX_VALUE - len)) {
			throw outOfRangeArgument("pos: " + pos);
		}

		final long endPos = (pos + len);
		char[] chars;

		if (pos > dlen) {
			// 1.) 'datachars' + '\32\32\32...' + substring
			chars = new char[(int) endPos - 1];
			data.getChars(0, dlen, chars, 0);
			for (int i = dlen; i < ipos; i++) {
				chars[i] = ' ';
			}
			str.getChars(offset, offset + len, chars, ipos);
		} else if (endPos > dlen) {
			// 2.) 'datach...' + substring
			chars = new char[(int) endPos - 1];
			data.getChars(0, ipos, chars, 0);
			str.getChars(offset, offset + len, chars, ipos);
		} else {
			// 3.) 'dat' + substring + 'rs'
			chars = new char[dlen];

			data.getChars(0, ipos, chars, 0);
			str.getChars(offset, offset + len, chars, ipos);
			final int dataOffset = ipos + len;
			data.getChars(dataOffset, dlen, chars, dataOffset);
		}

		setData(new String(chars));

		return len;
	}

	@Override
	public OutputStream setAsciiStream(final long pos) throws SQLException {

		checkReadonly();
		checkClosed();

		if (pos < MIN_POS || pos > MAX_POS) {
			throw outOfRangeArgument("pos: " + pos);
		}

		return new ByteArrayOutputStream() {
			boolean closed = false;

			public synchronized void close() throws IOException {
				if (closed) {
					return;
				}
				closed = true;
				final byte[] bytes = super.buf;
				final int length = super.count;
				super.buf = null;
				super.count = 0;
				try {
					final String str = new String(bytes, 0, length, "US-ASCII");
					Clob.this.setString(pos, str);
				} catch (SQLException se) {
					throw new IOException(se);
				}
			}
		};
	}

	@Override
	public Writer setCharacterStream(final long pos) throws SQLException {

		checkReadonly();
		checkClosed();

		if (pos < MIN_POS || pos > MAX_POS) {
			throw outOfRangeArgument("pos: " + pos);
		}

		return new StringWriter() {
			private boolean closed = false;

			public synchronized void close() throws IOException {
				if (closed) {
					return;
				}
				closed = true;
				final StringBuffer sb = super.getBuffer();
				try {
					Clob.this.setStringBuffer(pos, sb, 0, sb.length());
				} catch (SQLException se) {
					throw new IOException(se);
				} finally {
					sb.setLength(0);
					sb.trimToSize();
				}
			}
		};
	}

	@Override
	public void truncate(final long len) throws SQLException {
		checkReadonly();

		final String data = getData();
		final long dlen = data.length();

		if (len == dlen) {
			return;
		}

		if (len < 0 || len > dlen) {
			throw outOfRangeArgument("len: " + len);
		}

		setData(data.substring(0, (int) len));

	}

	@Override
	public synchronized void free() throws SQLException {
		m_closed = true;
		m_data = null;
	}

	@Override
	public Reader getCharacterStream(long pos, long length) throws SQLException {

		if (length > Integer.MAX_VALUE) {
			throw outOfRangeArgument("length: " + length);
		}

		final String data = getData();
		final int dlen = data.length();

		if (pos == MIN_POS && length == dlen) {
			return new StringReader(data);
		}

		if (pos < MIN_POS || pos > dlen) {
			throw outOfRangeArgument("pos: " + pos);
		}

		final long startIndex = pos - 1;

		if (length < 0 || length > dlen - startIndex) {
			throw outOfRangeArgument("length: " + length);
		}

		final int endIndex = (int) (startIndex + length); // exclusive
		final char[] chars = new char[(int) length];

		data.getChars((int) startIndex, endIndex, chars, 0);

		return new CharArrayReader(chars);
	}

	private static final long MIN_POS = 1L;
	private static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
	private boolean m_closed;
	private String m_data;
	private final boolean m_createdByConnection;

	public Clob(final String data) throws SQLException {

		if (data == null) {
			throw new SQLException("Null argument");
		}
		m_data = data;
		m_createdByConnection = false;
	}

	/**
	 * Constructs a new, empty (zero-length), read/write JDBCClob object.
	 */
	protected Clob() {
		m_data = "";
		m_createdByConnection = true;
	}

	protected void checkReadonly() throws SQLException {
		if (!m_createdByConnection) {
			throw new SQLException("Clob is read-only");
		}
	}

	protected synchronized void checkClosed() throws SQLException {

		if (m_closed) {
			throw new SQLException("Clob is closed");
		}
	}

	synchronized String getData() throws SQLException {

		checkClosed();

		return m_data;
	}

	private synchronized void setData(String data) throws SQLException {

		checkClosed();

		m_data = data;
	}

	public int setStringBuffer(final long pos, final StringBuffer sb, final int offset, final int len)
			throws SQLException {

		checkReadonly();

		String data = getData();

		if (sb == null) {
			throw nullArgument("sb");
		}

		final int strlen = sb.length();
		final int dlen = data.length();
		final int ipos = (int) (pos - 1);

		if (offset == 0 && len == strlen && ipos == 0 && len >= dlen) {
			setData(sb.toString());
			return len;
		}

		if (offset < 0 || offset > strlen) {
			throw outOfRangeArgument("offset: " + offset);
		}

		if (len > strlen - offset) {
			throw outOfRangeArgument("len: " + len);
		}

		if (pos < MIN_POS || (pos - MIN_POS) > (Integer.MAX_VALUE - len)) {
			throw outOfRangeArgument("pos: " + pos);
		}

		final long endPos = (pos + len);
		char[] chars;

		if (pos > dlen) {
			// 1.) 'datachars' + '\32\32\32...' + substring
			chars = new char[(int) endPos - 1];
			data.getChars(0, dlen, chars, 0);
			for (int i = dlen; i < ipos; i++) {
				chars[i] = ' ';
			}
			sb.getChars(offset, offset + len, chars, ipos);
		} else if (endPos > dlen) {
			// 2.) 'datach...' + substring
			chars = new char[(int) endPos - 1];
			data.getChars(0, ipos, chars, 0);
			sb.getChars(offset, offset + len, chars, ipos);
		} else {
			// 3.) 'dat' + substring + 'rs'
			chars = new char[dlen];

			data.getChars(0, ipos, chars, 0);
			sb.getChars(offset, offset + len, chars, ipos);
			final int dataOffset = ipos + len;
			data.getChars(dataOffset, dlen, chars, dataOffset);
		}

		setData(new String(chars));

		return len;
	}
}
