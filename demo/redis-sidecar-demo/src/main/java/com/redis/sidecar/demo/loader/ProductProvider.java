package com.redis.sidecar.demo.loader;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.github.javafaker.Faker;
import com.redis.sidecar.demo.Config.Loader;

public class ProductProvider implements RowProvider {

	private static final String[] PRODUCT_SCALES = { "1:10", "1:12", "1:16", "1:18" };
	private static final String[] PRODUCT_LINES = { "Classic Cars", "Motorcycles", "Planes", "Ships", "Trains",
			"Trucks and Buses", "Vintage Cars" };

	private final Faker faker = new Faker();

	@Override
	public void set(PreparedStatement statement, Loader config, int index) throws SQLException {
		double msrp = faker.number().randomDouble(2, 15, 1000);
		int columnIndex = 0;
		statement.setInt(++columnIndex, index + 1);
		statement.setString(++columnIndex, faker.commerce().productName());
		statement.setInt(++columnIndex, faker.random().nextInt(5, 5000));
		statement.setDouble(++columnIndex, msrp);
		statement.setDouble(++columnIndex, msrp * .6);
		statement.setString(++columnIndex, faker.company().name());
		statement.setString(++columnIndex, PRODUCT_LINES[faker.random().nextInt(PRODUCT_LINES.length)]);
		statement.setString(++columnIndex, PRODUCT_SCALES[faker.random().nextInt(PRODUCT_SCALES.length)]);
		statement.setString(++columnIndex, faker.lorem().paragraph(2));
	}

}