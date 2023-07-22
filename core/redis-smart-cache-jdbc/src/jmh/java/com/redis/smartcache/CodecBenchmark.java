package com.redis.smartcache;

import java.sql.SQLException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

public class CodecBenchmark {

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void decodeResultSet(CodecExecutionPlan plan) {
        plan.getCodec().decodeValue(plan.getBytesCodecByteBuffer());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void decodeResultSetMetadata(CodecExecutionPlan plan) throws SQLException {
        plan.getCodec().decodeMetaData(plan.getBytesCodecByteBuffer());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void encodeResultSet(CodecExecutionPlan plan) {
        plan.getCodec().encodeValue(plan.getRowSet());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void jdkDecodeResultSet(CodecExecutionPlan plan) {
        plan.getSerializedCodec().decodeValue(plan.getSerializedByteBuffer());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void jdkEncodeResultSet(CodecExecutionPlan plan) {
        plan.getSerializedCodec().encodeValue(plan.getRowSet());
    }

}
