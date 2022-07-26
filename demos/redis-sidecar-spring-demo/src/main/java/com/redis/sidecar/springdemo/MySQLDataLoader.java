package com.redis.sidecar.springdemo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.javafaker.Faker;
import com.redis.sidecar.springdemo.Config.Loader;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

@Component
public class MySQLDataLoader {

	private static final Logger log = LoggerFactory.getLogger(MySQLDataLoader.class);

	private static final String TABLE_ORDER_DETAILS = "orderdetails";
	private static final String TABLE_ORDERS = "orders";
	private static final String TABLE_CUSTOMERS = "customers";
	private static final String TABLE_PRODUCTS = "products";

	private static final InsertTask CUSTOMERS = new InsertTask(TABLE_CUSTOMERS, "customerNumber", "customerName",
			"contactLastName", "contactFirstName", "phone", "addressLine1", "addressLine2", "postalCode", "country");
	private static final InsertTask PRODUCTS = new InsertTask(TABLE_PRODUCTS, "productCode", "productName",
			"quantityInStock", "MSRP", "buyPrice", "productVendor", "productLine", "productScale",
			"productDescription");
	private static final InsertTask ORDERS = new InsertTask(TABLE_ORDERS, "orderNumber", "orderDate", "requiredDate",
			"shippedDate", "status", "customerNumber");
	private static final InsertTask ORDER_DETAILS = new InsertTask(TABLE_ORDER_DETAILS, "orderNumber", "productCode",
			"quantityOrdered", "orderLineNumber", "priceEach");

	private interface RowProvider {

		boolean hasNext();

		int getTotalRows();

		void setRow(PreparedStatement statement) throws SQLException;

	}

	private abstract static class AbstractRowProvider implements RowProvider {

		private final int totalRows;
		private int index = 0;

		protected AbstractRowProvider(int totalRows) {
			this.totalRows = totalRows;
		}

		@Override
		public int getTotalRows() {
			return totalRows;
		}

		@Override
		public boolean hasNext() {
			return index < totalRows;
		}

		@Override
		public void setRow(PreparedStatement statement) throws SQLException {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			doSetRow(statement, index);
			index++;
		}

		protected abstract void doSetRow(PreparedStatement statement, int index) throws SQLException;

		protected java.sql.Date sqlDate(Date date) {
			if (date == null) {
				return null;
			}
			return java.sql.Date.valueOf(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
		}

	}

	private static class OrderDetailsRowProvider extends AbstractRowProvider {

		private final Faker faker = new Faker();
		private final int itemsPerOrder;

		public OrderDetailsRowProvider(int totalRows, int itemsPerOrder) {
			super(totalRows);
			this.itemsPerOrder = itemsPerOrder;
		}

		@Override
		protected void doSetRow(PreparedStatement statement, int index) throws SQLException {
			int orderNumber = (index / itemsPerOrder) + 1;
			int productCode = (index % itemsPerOrder) + 1;
			int columnIndex = 0;
			statement.setInt(++columnIndex, orderNumber);
			statement.setInt(++columnIndex, productCode);
			statement.setInt(++columnIndex, faker.random().nextInt(1, 50));
			statement.setInt(++columnIndex, faker.random().nextInt(1, 4));
			statement.setDouble(++columnIndex, faker.number().randomDouble(2, 15, 1000));
		}

	}

	private static class OrderRowProvider extends AbstractRowProvider {

		private static final String ORDER_STATUS_SHIPPED = "Shipped";
		private static final String[] ORDER_STATUSES = { "In Process", ORDER_STATUS_SHIPPED, "Disputed", "Resolved" };

		private final Faker faker = new Faker();
		private final int customerRows;

		public OrderRowProvider(int totalRows, int customerRows) {
			super(totalRows);
			this.customerRows = customerRows;
		}

		@Override
		protected void doSetRow(PreparedStatement statement, int index) throws SQLException {
			Date orderDate = faker.date().past(750, 1, TimeUnit.DAYS);
			Calendar requiredDate = Calendar.getInstance();
			requiredDate.setTime(orderDate);
			requiredDate.add(Calendar.DAY_OF_MONTH, faker.random().nextInt(3, 10));
			String status = ORDER_STATUSES[faker.random().nextInt(ORDER_STATUSES.length)];
			Date shippedDate = status.equals(ORDER_STATUS_SHIPPED) ? requiredDate.getTime() : null;
			int customerNumber = faker.random().nextInt(1, customerRows);
			int columnIndex = 0;
			statement.setInt(++columnIndex, index + 1);
			statement.setDate(++columnIndex, sqlDate(orderDate));
			statement.setDate(++columnIndex, sqlDate(requiredDate.getTime()));
			statement.setDate(++columnIndex, sqlDate(shippedDate));
			statement.setString(++columnIndex, status);
			statement.setInt(++columnIndex, customerNumber);
		}

	}

	private static class CustomerRowProvider extends AbstractRowProvider {

		private final Faker faker = new Faker();

		public CustomerRowProvider(int totalRows) {
			super(totalRows);
		}

		@Override
		protected void doSetRow(PreparedStatement statement, int index) throws SQLException {
			int columnIndex = 0;
			statement.setInt(++columnIndex, index + 1);
			statement.setString(++columnIndex, faker.company().name());
			statement.setString(++columnIndex, faker.name().lastName());
			statement.setString(++columnIndex, faker.name().lastName());
			statement.setString(++columnIndex, faker.phoneNumber().phoneNumber());
			statement.setString(++columnIndex, faker.address().streetAddress());
			statement.setString(++columnIndex, faker.address().secondaryAddress());
			statement.setString(++columnIndex, faker.address().zipCode());
			statement.setString(++columnIndex, faker.address().country());
		}

	}

	private static class ProductRowProvider extends AbstractRowProvider {

		private static final String[] PRODUCT_SCALES = { "1:10", "1:12", "1:16", "1:18" };
		private static final String[] PRODUCT_LINES = { "Classic Cars", "Motorcycles", "Planes", "Ships", "Trains",
				"Trucks and Buses", "Vintage Cars" };

		private final Faker faker = new Faker();

		public ProductRowProvider(int totalRows) {
			super(totalRows);
		}

		@Override
		protected void doSetRow(PreparedStatement statement, int index) throws SQLException {
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

	private final Loader config;
	private final DataSource dataSource;

	public MySQLDataLoader(Config config, DataSource dataSource) {
		this.config = config.getLoader();
		this.dataSource = dataSource;
	}

	@PostConstruct
	public void execute() throws SQLException {
		CUSTOMERS.execute(new CustomerRowProvider(config.getCustomers()), dataSource, config.getBatch());
		PRODUCTS.execute(new ProductRowProvider(config.getProducts()), dataSource, config.getBatch());
		ORDERS.execute(new OrderRowProvider(config.getOrders(), config.getCustomers()), dataSource, config.getBatch());
		ORDER_DETAILS.execute(
				new OrderDetailsRowProvider(config.getOrderDetails(), config.getOrderDetails() / config.getOrders()),
				dataSource, config.getBatch());
	}

	private static class InsertTask {

		private final String table;
		private List<String> columns;

		private InsertTask(String table, String... columns) {
			this.table = table;
			this.columns = Arrays.asList(columns);
		}

		public void execute(RowProvider rowProvider, DataSource dataSource, int batchSize) throws SQLException {
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(false);
				try (Statement statement = connection.createStatement()) {
					ResultSet countResultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table);
					countResultSet.next();
					if (countResultSet.getInt(1) == rowProvider.getTotalRows()) {
						log.info("Table {} already populated", table);
						return;
					}
				}
				ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
				progressBarBuilder.setInitialMax(rowProvider.getTotalRows());
				progressBarBuilder.setTaskName("Populating " + table);
				progressBarBuilder.showSpeed();
				try (ProgressBar progressBar = progressBarBuilder.build()) {
					log.info("Populating {}", table);
					String insertSQL = String.format("INSERT INTO %s %s VALUES %s", table,
							columns.stream().collect(Collectors.joining(",", "(", ")")),
							columns.stream().map(n -> "?").collect(Collectors.joining(",", "(", ")")));
					PreparedStatement statement = connection.prepareStatement(insertSQL);
					int index = 0;
					while (rowProvider.hasNext()) {
						rowProvider.setRow(statement);
						statement.addBatch();
						index++;
						if (index > 0 && index % batchSize == 0) {
							statement.executeBatch();
							connection.commit();
							statement.close();
							statement = statement.getConnection().prepareStatement(insertSQL);
							progressBar.stepTo(index);
						}
					}
					statement.executeBatch();
					connection.commit();
					statement.close();
				}
			}
		}

	}

}
