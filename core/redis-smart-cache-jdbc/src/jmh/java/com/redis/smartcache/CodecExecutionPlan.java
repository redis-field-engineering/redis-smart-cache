package com.redis.smartcache;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.redis.smartcache.jdbc.RowSetCodec;
import com.redis.smartcache.jdbc.codec.SerializedResultSetCodec;
import com.redis.smartcache.test.RowSetBuilder;

@State(Scope.Benchmark)
public class CodecExecutionPlan {

    private static final int BYTE_BUFFER_CAPACITY = 100 * 1024 * 1024;

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
        this.codec = new RowSetCodec(BYTE_BUFFER_CAPACITY);
        this.serializedCodec = new SerializedResultSetCodec(BYTE_BUFFER_CAPACITY);
    }

    @Setup(Level.Invocation)
    public void setUpInvocation() throws SQLException {
        RowSetBuilder rowSetBuilder = RowSetBuilder.of(new RowSetFactoryImpl()).rowCount(rows).columnCount(columns);
        this.rowSet = rowSetBuilder.build();
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
