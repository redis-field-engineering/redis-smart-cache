package com.redis.sidecar;

import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SqlParser {

	public List<String> getTableList(String sql) throws JSQLParserException {
		Statement parsedStatement = CCJSqlParserUtil.parse(sql);
		if (parsedStatement instanceof Select) {
			return new TablesNamesFinder().getTableList(parsedStatement);
		}
		return Collections.emptyList();
	}

}
