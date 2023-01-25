package com.redis.sidecar;

import java.io.IOException;
import java.sql.SQLException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

public class CodecBenchmark {

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitDecode(CodecExecutionPlan plan) {
		plan.getExplicitCodec().decodeValue(plan.getExplicitByteBufer());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitDecodeMetadata(CodecExecutionPlan plan) {
		plan.getExplicitCodec().decodeValue(plan.getExplicitByteBufer());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void explicitEncode(CodecExecutionPlan plan) throws SQLException {
		plan.getExplicitCodec().encodeValue(plan.newRowSet()).array();
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void jdkDecode(CodecExecutionPlan plan) throws IOException, SQLException, ClassNotFoundException {
		plan.getJdkCodec().decode(plan.getJdkBytes());
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void jdkEncode(CodecExecutionPlan plan) throws IOException, SQLException {
		plan.getJdkCodec().encode(plan.newRowSet());
	}

}
