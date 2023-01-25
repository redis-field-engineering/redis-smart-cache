package com.redis.sidecar.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.github.javafaker.Faker;
import com.redis.sidecar.demo.Config.Loader;

public class CustomerProvider implements RowProvider {
	
	private final Faker faker = new Faker();

	@Override
	public void set(PreparedStatement statement, Loader config, int index) throws SQLException {
		int columnIndex = 1;
		statement.setInt(columnIndex++, index + 1);
		statement.setString(columnIndex++, faker.company().name());
		statement.setString(columnIndex++, faker.name().lastName());
		statement.setString(columnIndex++, faker.name().lastName());
		statement.setString(columnIndex++, faker.phoneNumber().phoneNumber());
		statement.setString(columnIndex++, faker.address().streetAddress());
		statement.setString(columnIndex++, faker.address().secondaryAddress());
		statement.setString(columnIndex++, faker.address().zipCode());
		statement.setString(columnIndex, faker.address().country());
	}

}