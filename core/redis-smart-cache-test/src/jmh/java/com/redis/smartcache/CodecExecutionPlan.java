package com.redis.smartcache;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.redis.smartcache.core.codec.RowSetCodec;
import com.redis.smartcache.core.codec.SerializedResultSetCodec;
import com.redis.smartcache.core.rowset.CachedRowSetFactory;
import com.redis.smartcache.test.RowSetBuilder;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

@State(Scope.Benchmark)
public class CodecExecutionPlan {

	private static final DataSize BYTE_BUFFER_CAPACITY = DataSize.of(100, Unit.MEGABYTE);

	@Param({ "10", "100" })
	private int columns;
	@Param({ "10", "100", "1000" })
	private int rows;

	private RowSetCodec codec;
	private SerializedResultSetCodec serializedCodec;
	private ByteBuffer byteBuffer;
	private ByteBuffer serializedByteBuffer;
	private CachedRowSet rowSet;

	@Setup(Level.Trial)
	public void setUpTrial() {
		this.codec = RowSetCodec.builder().maxByteBufferCapacity(BYTE_BUFFER_CAPACITY).build();
		this.serializedCodec = new SerializedResultSetCodec(new CachedRowSetFactory(),
				Math.toIntExact(BYTE_BUFFER_CAPACITY.toBytes()));
	}

	@Setup(Level.Invocation)
	public void setUpInvocation() throws SQLException {
		RowSetBuilder rowSetBuilder = new RowSetBuilder();
		this.rowSet = rowSetBuilder.build(rowSetBuilder.metaData(columns, RowSetBuilder.SUPPORTED_TYPES), rows);
		rowSet.beforeFirst();
		this.byteBuffer = codec.encodeValue(rowSet);
		rowSet.beforeFirst();
		this.serializedByteBuffer = serializedCodec.encodeValue(rowSet);
		rowSet.beforeFirst();
	}

	public int getColumns() {
		return columns;
	}

	public int getRows() {
		return rows;
	}

	public CachedRowSet getRowSet() {
		return rowSet;
	}

	public RowSetCodec getCodec() {
		return codec;
	}

	public ByteBuffer getBytesCodecByteBuffer() {
		return byteBuffer;
	}

	public SerializedResultSetCodec getSerializedCodec() {
		return serializedCodec;
	}

	public ByteBuffer getSerializedByteBuffer() {
		return serializedByteBuffer;
	}
}