package com.redis.smartcache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.Config.RulesetConfig.RuleConfig;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryRuleSession;
import com.redis.smartcache.jdbc.SmartConnection;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;

class RulesTests {

	private static final String PRODUCTS = "products";
	private static final String PRODUCTS_P = PRODUCTS + " p";
	private static final String CUSTOMERS = "customers";
	private static final String CUSTOMERS_C = CUSTOMERS + " c";
	private static final String ORDERS = "orders";
	private static final String ORDERS_O = ORDERS + " o";

	private static final SqlParser PARSER = new SqlParser();
	private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions();

	@Test
	void testTables() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tables(PRODUCTS, CUSTOMERS, ORDERS).ttl(ttl).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Query statement = query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O);
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C);
		ruleSession.fire(statement);
		Assertions.assertEquals(Query.TTL_NO_CACHING, statement.getTtl());
	}

	@Test
	void testTablesAny() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tablesAny(PRODUCTS, CUSTOMERS).ttl(ttl).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Query statement = query("SELECT * FROM " + PRODUCTS_P);
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = query("SELECT * FROM " + ORDERS_O);
		ruleSession.fire(statement);
		Assertions.assertEquals(Query.TTL_NO_CACHING, statement.getTtl());
	}

	@Test
	void testTablesAll() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tablesAll(PRODUCTS, CUSTOMERS).ttl(ttl).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Query query = query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O);
		ruleSession.fire(query);
		Assertions.assertEquals(ttl, query.getTtl());
		query = query("SELECT * FROM " + ORDERS_O);
		ruleSession.fire(query);
		Assertions.assertEquals(Query.TTL_NO_CACHING, query.getTtl());
	}

	@Test
	void testRegex() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.regex("SELECT\\s+\\*\\s+FROM\\s+.*").ttl(ttl).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Query statement = query("SELECT * FROM blah");
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = query("SELECT COUNT(*) FROM blah");
		ruleSession.fire(statement);
		Assertions.assertEquals(Query.TTL_NO_CACHING, statement.getTtl());
	}

	private Query query(String sql) {
		return new Query(SmartConnection.crc32(sql), sql, PARSER.createStatement(sql, PARSING_OPTIONS), null, null,
				null, null, null, null);
	}

}
