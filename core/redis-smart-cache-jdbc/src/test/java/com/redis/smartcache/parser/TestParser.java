package com.redis.smartcache.parser;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

class TestParser {

	private static final SqlParser parser = new SqlParser();

	public static void main(String[] args) {
		String sql = "INSERT INTO Product(productid, productname, supplierid, categoryid, unitprice, discontinued) VALUES(99999, 'Product OFBNT', 12, 7, 45, 1)";
		Statement statement = parser.createStatement(sql, new ParsingOptions());
		System.out.println(SqlFormatter.formatSql(statement));
	}

}
