package com.redis.sidecar;

import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SqlParser {

	public Statement parse(String sql) throws JSQLParserException {
		return CCJSqlParserUtil.parse(sql,
				parser -> parser.withSquareBracketQuotation(true).withAllowComplexParsing(false));
	}

	public List<String> getTableList(String sql) throws JSQLParserException {
		Statement statement = parse(sql);
		if (statement instanceof Select) {
			return new TablesNamesFinder().getTableList(statement);
		}
		return Collections.emptyList();
	}

}
