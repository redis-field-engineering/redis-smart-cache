package com.redis.smartcache.core;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.core.Config.RuleConfig;
import com.redis.smartcache.core.Config.RulesetConfig;

import io.airlift.units.Duration;

class RulesTests {

	private static final String PRODUCTS = "products";
	private static final String PRODUCTS_P = PRODUCTS + " p";
	private static final String CUSTOMERS = "customers";
	private static final String CUSTOMERS_C = CUSTOMERS + " c";
	private static final String ORDERS = "orders";
	private static final String ORDERS_O = ORDERS + " o";

	private static final SQLParser PARSER = new SQLParser();
	private static final Duration DEFAULT_TTL = new Duration(123, TimeUnit.SECONDS);

	@Test
	void testTables() {
		RuleConfig rule = RuleConfig.tables(PRODUCTS, CUSTOMERS, ORDERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(), ruleSession
				.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O)).getTtl().toMillis());
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C)).isCaching());
	}

	@Test
	void testTablesAny() {
		RuleConfig rule = RuleConfig.tablesAny(PRODUCTS, CUSTOMERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(),
				ruleSession.fire(query("SELECT * FROM " + PRODUCTS_P)).getTtl().toMillis());
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM " + ORDERS_O)).isCaching());
	}

	@Test
	void testTablesAll() {
		RuleConfig rule = RuleConfig.tablesAll(PRODUCTS, CUSTOMERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(), ruleSession
				.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O)).getTtl().toMillis());
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM " + ORDERS_O)).isCaching());
	}

	@Test
	void testRegex() {
		RuleConfig rule = RuleConfig.regex("SELECT\\s+\\*\\s+FROM\\s+.*").ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(),
				ruleSession.fire(query("SELECT * FROM blah")).getTtl().toMillis());
		Assertions.assertFalse(ruleSession.fire(query("SELECT COUNT(*) FROM blah")).isCaching());
	}

	@Test
	void testQueryIds() {
		String sql = "SELECT * FROM blah";
		RuleConfig rule = RuleConfig.queryIds(String.valueOf(HashingFunctions.crc32(sql))).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(), ruleSession.fire(query(sql)).getTtl().toMillis());
		Assertions.assertFalse(ruleSession.fire(query("SELECT COUNT(*) FROM blah")).isCaching());
	}

	@Test
	void testDisableCaching() {
		RuleConfig rule = RuleConfig.passthrough().ttl(Duration.valueOf("0s")).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM blah")).isCaching());
	}

	private Query query(String sql) {
		Query query = new Query();
		query.setId(String.valueOf(HashingFunctions.crc32(sql)));
		query.setSql(sql);
		query.setTables(PARSER.extractTableNames(sql));
		return query;
	}

}
