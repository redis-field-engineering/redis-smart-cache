package com.redis.sidecar.springdemo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.github.javafaker.Faker;
import com.redis.sidecar.springdemo.Config.Loader;

public class OrderProvider implements RowProvider {

	private static final String ORDER_STATUS_SHIPPED = "Shipped";
	private static final String[] ORDER_STATUSES = { "In Process", ORDER_STATUS_SHIPPED, "Disputed", "Resolved" };

	private final Faker faker = new Faker();

	@Override
	public void set(PreparedStatement statement, Loader config, int index) throws SQLException {
		Date orderDate = faker.date().past(750, 1, TimeUnit.DAYS);
		Calendar requiredDate = Calendar.getInstance();
		requiredDate.setTime(orderDate);
		requiredDate.add(Calendar.DAY_OF_MONTH, faker.random().nextInt(3, 10));
		String status = ORDER_STATUSES[faker.random().nextInt(ORDER_STATUSES.length)];
		Date shippedDate = status.equals(ORDER_STATUS_SHIPPED) ? requiredDate.getTime() : null;
		int customerNumber = faker.random().nextInt(1, config.getCustomers());
		int columnIndex = 1;
		statement.setInt(columnIndex++, index + 1);
		statement.setDate(columnIndex++, RowProvider.sqlDate(orderDate));
		statement.setDate(columnIndex++, RowProvider.sqlDate(requiredDate.getTime()));
		statement.setDate(columnIndex++, RowProvider.sqlDate(shippedDate));
		statement.setString(columnIndex++, status);
		statement.setInt(columnIndex, customerNumber);
	}

}