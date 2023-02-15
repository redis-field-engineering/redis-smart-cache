package com.redis.smartcache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import com.redis.smartcache.core.Query;

public class SqlParserBenchmark {

	private static final String KEY = "123";
	private static final String SQL = "SELECT orders.orderNumber, orders.orderDate, orders.requiredDate, orders.shippedDate, orders.status, orders.customerNumber, customers.customerName, orderdetails.productCode, products.productName, orderdetails.quantityOrdered FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber JOIN products ON orderdetails.productCode = products.productCode WHERE orders.orderNumber = ?";

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	public void trinoParser() {
		Query.of(KEY, SQL);
	}

}
