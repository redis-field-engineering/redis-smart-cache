package com.redis.sidecar;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import net.sf.jsqlparser.JSQLParserException;

public class SqlParserBenchmark {

	private static final SqlParser PARSER = new SqlParser();
	private static final String SQL = "SELECT SLEEP(1), orders.orderNumber, orders.orderDate, orders.requiredDate, orders.shippedDate, orders.status, orders.customerNumber, customers.customerName, orderdetails.productCode, products.productName, orderdetails.quantityOrdered FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber JOIN products ON orderdetails.productCode = products.productCode WHERE orders.orderNumber = ?";

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void parse() throws JSQLParserException {
		PARSER.parse(SQL);
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void getTableList() throws JSQLParserException {
		PARSER.getTableList(SQL);
	}
}
