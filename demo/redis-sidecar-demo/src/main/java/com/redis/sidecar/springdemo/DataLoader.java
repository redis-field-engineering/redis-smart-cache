package com.redis.sidecar.springdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.redis.sidecar.springdemo.Config.Loader;
import com.redis.sidecar.springdemo.loader.CustomerProvider;
import com.redis.sidecar.springdemo.loader.OrderDetailsProvider;
import com.redis.sidecar.springdemo.loader.OrderProvider;
import com.redis.sidecar.springdemo.loader.ProductProvider;
import com.redis.sidecar.springdemo.loader.RowProvider;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

@Component
public class DataLoader {

	public static final String PRODUCTS = "products";
	public static final String CUSTOMERS = "customers";
	public static final String ORDERS = "orders";
	public static final String ORDERDETAILS = "orderdetails";

	public static final Logger log = LoggerFactory.getLogger(DataLoader.class);

	private static final String[] PRODUCT_COLUMNS = { "productCode", "productName", "quantityInStock", "MSRP",
			"buyPrice", "productVendor", "productLine", "productScale", "productDescription" };
	private static final String[] CUSTOMER_COLUMNS = { "customerNumber", "customerName", "contactLastName",
			"contactFirstName", "phone", "addressLine1", "addressLine2", "postalCode", "country" };
	private static final String[] ORDER_COLUMNS = { "orderNumber", "orderDate", "requiredDate", "shippedDate", "status",
			"customerNumber" };
	private static final String[] ORDERDETAILS_COLUMNS = { "orderNumber", "productCode", "quantityOrdered",
			"orderLineNumber", "priceEach" };

	private final Loader config;
	private final DataSource dataSource;

	public DataLoader(Config config, DataSource dataSource) {
		this.config = config.getLoader();
		this.dataSource = dataSource;
	}

	public void execute() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			load(connection, CUSTOMERS, CUSTOMER_COLUMNS, new CustomerProvider(), config.getCustomers());
			load(connection, PRODUCTS, PRODUCT_COLUMNS, new ProductProvider(), config.getProducts());
			load(connection, ORDERS, ORDER_COLUMNS, new OrderProvider(), config.getOrders());
			load(connection, ORDERDETAILS, ORDERDETAILS_COLUMNS, new OrderDetailsProvider(), config.getOrderdetails());
		}
	}

	private void load(Connection connection, String table, String[] columns, RowProvider rowProvider, int end)
			throws SQLException {
		int start;
		try (Statement statement = connection.createStatement()) {
			ResultSet countResultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table);
			countResultSet.next();
			start = countResultSet.getInt(1);
			if (start >= end) {
				return;
			}
		}
		connection.setAutoCommit(false);
		ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
		progressBarBuilder.setInitialMax(end);
		progressBarBuilder.setTaskName(String.format("Populating %s", table));
		progressBarBuilder.showSpeed();
		progressBarBuilder.startsFrom(start, Duration.ZERO);
		ProgressBar progressBar = progressBarBuilder.build();
		String insertSQL = String.format("INSERT INTO %s %s VALUES %s", table,
				Stream.of(columns).collect(Collectors.joining(",", "(", ")")),
				Stream.of(columns).map(n -> "?").collect(Collectors.joining(",", "(", ")")));
		PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
		for (int index = start; index < end; index++) {
			rowProvider.set(preparedStatement, config, index);
			preparedStatement.addBatch();
			if (index > 0 && index % config.getBatch() == 0) {
				preparedStatement.executeBatch();
				connection.commit();
				preparedStatement.close();
				preparedStatement = preparedStatement.getConnection().prepareStatement(insertSQL);
				progressBar.stepBy(config.getBatch());
			}
		}
		preparedStatement.executeBatch();
		connection.commit();
		preparedStatement.close();
		progressBar.stepTo(end);
		progressBar.close();
		connection.setAutoCommit(true);
	}

}
