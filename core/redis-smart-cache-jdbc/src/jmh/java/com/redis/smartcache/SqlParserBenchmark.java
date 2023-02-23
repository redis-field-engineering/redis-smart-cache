package com.redis.smartcache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;

public class SqlParserBenchmark {

	private static final SqlParser parser = new SqlParser();
	private static final ParsingOptions parsingOptions = new ParsingOptions();

	private static final String SQL = "SELECT orders.orderNumber, orders.orderDate, orders.requiredDate, orders.shippedDate, orders.status, orders.customerNumber, customers.customerName, orderdetails.productCode, products.productName, orderdetails.quantityOrdered FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber JOIN products ON orderdetails.productCode = products.productCode WHERE orders.orderNumber = ?";

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void parseSQL() {
		parser.createStatement(SQL, parsingOptions);
	}
	
	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void extractTableNames() {
		parser.createStatement(SQL, parsingOptions);
	}


}
