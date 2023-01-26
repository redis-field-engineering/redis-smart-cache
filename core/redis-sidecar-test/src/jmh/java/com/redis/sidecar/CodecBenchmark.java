package com.redis.sidecar;

import java.sql.SQLException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

public class CodecBenchmark {

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitDecode(CodecExecutionPlan plan) {
		plan.getExplicitCodec().decodeValue(plan.getExplicitByteBuffer());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitDecodeMetadata(CodecExecutionPlan plan) throws SQLException {
		plan.getExplicitCodec().decodeMetaData(plan.getExplicitByteBuffer());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitEncode(CodecExecutionPlan plan) {
		plan.getExplicitCodec().encodeValue(plan.getRowSet());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void jdkDecode(CodecExecutionPlan plan) {
		plan.getJdkCodec().decodeValue(plan.getJdkByteBuffer());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void jdkEncode(CodecExecutionPlan plan) {
		plan.getJdkCodec().encodeValue(plan.getRowSet());
	}

}
