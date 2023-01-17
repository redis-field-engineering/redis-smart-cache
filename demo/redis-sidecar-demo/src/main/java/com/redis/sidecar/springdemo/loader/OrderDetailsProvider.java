package com.redis.sidecar.springdemo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.github.javafaker.Faker;
import com.redis.sidecar.springdemo.Config.Loader;

public class OrderDetailsProvider implements RowProvider {

	private final Faker faker = new Faker();

	@Override
	public void set(PreparedStatement statement, Loader config, int index) throws SQLException {
		int itemsPerOrder = config.getOrderdetails() / config.getOrders();
		int orderNumber = index / itemsPerOrder + 1;
		int productCode = index % itemsPerOrder + 1;
		int columnIndex = 1;
		statement.setInt(columnIndex++, orderNumber);
		statement.setInt(columnIndex++, productCode);
		statement.setInt(columnIndex++, faker.random().nextInt(1, 50));
		statement.setInt(columnIndex++, faker.random().nextInt(1, 4));
		statement.setDouble(columnIndex, faker.number().randomDouble(2, 15, 1000));
	}

}