package com.redis.smartcache.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.redis.smartcache.demo.DemoConfig.DataConfig;

import net.datafaker.Faker;

public class OrderDetailsProvider implements RowProvider {

	private final Faker faker = new Faker();

	@Override
	public void set(PreparedStatement statement, DataConfig config, int index) throws SQLException {
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