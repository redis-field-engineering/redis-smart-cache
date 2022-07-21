package com.redis.sidecar.springdemo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import com.redis.sidecar.springdemo.Config.Loader;

@Component
@ConditionalOnExpression(value = "#{ '${database}' matches 'postgres' }")
public class PostgresDataLoader {

	private static Logger log = LoggerFactory.getLogger(PostgresDataLoader.class);

	private static final List<String> CUSTOMER_IDS = Arrays.asList("ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "BLAUS",
			"BLONP", "BOLID", "BONAP", "BOTTM", "BSBEV", "CACTU", "CENTC", "CHOPS", "COMMI", "CONSH", "DRACD", "DUMON",
			"EASTC", "ERNSH", "FAMIA", "FISSA", "FOLIG", "FOLKO", "FRANK", "FRANR", "FRANS", "FURIB", "GALED", "GODOS",
			"GOURL", "GREAL", "GROSR", "HANAR", "HILAA", "HUNGC", "HUNGO", "ISLAT", "KOENE", "LACOR", "LAMAI", "LAUGB",
			"LAZYK", "LEHMS", "LETSS", "LILAS", "LINOD", "LONEP", "MAGAA", "MAISD", "MEREP", "MORGK", "NORTS", "OCEAN",
			"OLDWO", "OTTIK", "PARIS", "PERIC", "PICCO", "PRINI", "QUEDE", "QUEEN", "QUICK", "RANCH", "RATTC", "REGGC",
			"RICAR", "RICSU", "ROMEY", "SANTG", "SAVEA", "SEVES", "SIMOB", "SPECD", "SPLIR", "SUPRD", "THEBI", "THECR",
			"TOMSP", "TORTU", "TRADH", "TRAIH", "VAFFE", "VICTE", "VINET", "WANDK", "WARTH", "WELLI", "WHITC", "WILMK",
			"WOLZA");

	private final Random random = new Random();
	private final Loader config;
	private final DataSource dataSource;

	public PostgresDataLoader(Config config, DataSource dataSource) {
		this.config = config.getLoader();
		this.dataSource = dataSource;
	}

	@PostConstruct
	public void execute() throws SQLException, IOException {
		String insertOrderSQL = "INSERT INTO orders VALUES (?, ?, ?, '1996-07-04', '1996-08-01', '1996-07-16', 3, 32.3800011, 'Vins et alcools Chevalier', '59 rue de l''Abbaye', 'Reims', NULL, '51100', 'France')";
		String insertOrderDetailsSQL = "INSERT INTO order_details VALUES (?, ?, ?, ?, 0)";
		try (Connection connection = dataSource.getConnection()) {
			ScriptRunner scriptRunner = new ScriptRunner(connection);
			scriptRunner.setAutoCommit(false);
			scriptRunner.setStopOnError(true);
			if (config.isDrop()) {
				try (InputStream inputStream = getClass().getClassLoader()
						.getResourceAsStream("db/postgres/drop.sql")) {
					scriptRunner.runScript(new InputStreamReader(inputStream));
				}
			}
			if (rowCount() == config.getRows()) {
				log.info("Database already populated");
				return;
			}
			try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("db/postgres/schema.sql")) {
				scriptRunner.runScript(new InputStreamReader(inputStream));
			}
			try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("db/postgres/data.sql")) {
				scriptRunner.runScript(new InputStreamReader(inputStream));
			}
			PreparedStatement orderStatement = connection.prepareStatement(insertOrderSQL);
			PreparedStatement detailsStatement = connection.prepareStatement(insertOrderDetailsSQL);
			log.info("Populating database with {} rows", config.getRows());
			for (int index = 0; index < config.getRows(); index++) {
				String customerId = CUSTOMER_IDS.get(random.nextInt(CUSTOMER_IDS.size()));
				int employeeId = 1 + random.nextInt(9);
				int productId = 1 + random.nextInt(77);
				double price = random.nextDouble();
				int quantity = random.nextInt(100);

				orderStatement.setInt(1, index);
				orderStatement.setString(2, customerId);
				orderStatement.setInt(3, employeeId);
				orderStatement.addBatch();

				detailsStatement.setInt(1, index);
				detailsStatement.setInt(2, productId);
				detailsStatement.setDouble(3, price);
				detailsStatement.setInt(4, quantity);
				detailsStatement.addBatch();

				if (index % config.getBatch() == 0) {
					orderStatement.executeBatch();
					orderStatement.close();
					orderStatement = connection.prepareStatement(insertOrderSQL);
					detailsStatement.executeBatch();
					detailsStatement.close();
					detailsStatement = connection.prepareStatement(insertOrderDetailsSQL);
					log.info(String.format("Inserted %,d/%,d rows", index, config.getRows()));
				}
			}
			orderStatement.executeBatch();
			orderStatement.close();
			detailsStatement.executeBatch();
			detailsStatement.close();
			log.info("Inserted {} rows", rowCount());
		}

	}

	private int rowCount() throws SQLException {
		try (Connection connection = dataSource.getConnection();
				Statement countStatement = connection.createStatement();
				ResultSet countResultSet = countStatement.executeQuery("SELECT COUNT(*) FROM orders");) {
			countResultSet.next();
			return countResultSet.getInt(1);
		} catch (SQLException e) {
			return 0;
		}

	}

}
