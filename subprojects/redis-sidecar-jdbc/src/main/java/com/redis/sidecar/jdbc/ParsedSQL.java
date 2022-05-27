package com.redis.sidecar.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class ParsedSQL {

	private static final Logger log = Logger.getLogger(ParsedSQL.class.getName());

	private final String sql;
	private final List<String> tables;

	public ParsedSQL(String sql) {
		this(sql, Collections.emptyList());
	}

	public ParsedSQL(String sql, List<String> tables) {
		this.sql = sql;
		this.tables = tables;
	}

	public String getSQL() {
		return sql;
	}

	public List<String> getTables() {
		return tables;
	}

	public static ParsedSQL parse(String sql) {
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement instanceof Select) {
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				List<String> tableNames = tablesNamesFinder.getTableList((Select) statement);
				return new ParsedSQL(sql, tableNames);
			}
		} catch (JSQLParserException e) {
			log.log(Level.FINE, "Could not parse sql: " + sql, e);
		}
		return new ParsedSQL(sql);

	}
}
