package com.redis.smartcache.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.core.config.RulesetConfig;

import io.airlift.units.Duration;

class RulesTests {

	private static final Duration DURATION_0S = Duration.valueOf("0s");
	private static final Duration DURATION_300S = Duration.valueOf("300s");
	private static final String PRODUCTS = "products";
	private static final String PRODUCTS_P = PRODUCTS + " p";
	private static final String CUSTOMERS = "customers";
	private static final String CUSTOMERS_C = CUSTOMERS + " c";
	private static final String ORDERS = "orders";
	private static final String ORDERS_O = ORDERS + " o";

	private static final Duration DEFAULT_TTL = new Duration(123, TimeUnit.SECONDS);
	private static String BLAH_TABLE = "blah";
	private static String BLAH_SQL = "SELECT * FROM " + BLAH_TABLE;
	private static final Query BLAH_QUERY = query(BLAH_SQL, BLAH_TABLE);

	@Test
	void testTables() {
		RuleConfig rule = RuleConfig.tables(PRODUCTS, CUSTOMERS, ORDERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(),
				ruleSession.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O, PRODUCTS,
						CUSTOMERS, ORDERS, PRODUCTS)).getTtl());
		Assertions.assertFalse(ruleSession
				.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS, PRODUCTS, CUSTOMERS)).getTtl() > 0);
	}

	@Test
	void testTablesAny() {
		RuleConfig rule = RuleConfig.tablesAny(PRODUCTS, CUSTOMERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(),
				ruleSession.fire(query("SELECT * FROM " + PRODUCTS_P, PRODUCTS)).getTtl());
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM " + ORDERS_O, ORDERS)).getTtl() > 0);
	}

	@Test
	void testTablesAll() {
		RuleConfig rule = RuleConfig.tablesAll(PRODUCTS, CUSTOMERS).ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(),
				ruleSession.fire(query("SELECT * FROM " + PRODUCTS_P + ", " + CUSTOMERS_C + ", " + ORDERS_O, PRODUCTS,
						CUSTOMERS, ORDERS)).getTtl());
		Assertions.assertFalse(ruleSession.fire(query("SELECT * FROM " + ORDERS_O, ORDERS)).getTtl() > 0);
	}

	@Test
	void testRegex() {
		RuleConfig rule = RuleConfig.regex("SELECT\\s+\\*\\s+FROM\\s+.*").ttl(DEFAULT_TTL).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(), ruleSession.fire(BLAH_QUERY).getTtl());
		Assertions.assertFalse(ruleSession.fire(query("SELECT COUNT(*) FROM " + BLAH_TABLE, BLAH_TABLE)).getTtl() > 0);
	}

	@Test
	void testQueryIds() {
		RuleConfig rule = RuleConfig.queryIds(String.valueOf(HashingFunctions.crc32(BLAH_SQL))).ttl(DEFAULT_TTL)
				.build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertEquals(DEFAULT_TTL.toMillis(), ruleSession.fire(BLAH_QUERY).getTtl());
		Assertions.assertFalse(ruleSession.fire(query("SELECT COUNT(*) FROM " + BLAH_TABLE, BLAH_TABLE)).getTtl() > 0);
	}

	@Test
	void testDisableCaching() {
		RuleConfig rule = RuleConfig.passthrough().ttl(DURATION_0S).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule));
		Assertions.assertFalse(ruleSession.fire(BLAH_QUERY).getTtl() > 0);
	}

	@Test
	void testMultipleRules() {
		RuleConfig rule1 = RuleConfig.passthrough().ttl(DURATION_0S).build();
		RuleConfig rule2 = RuleConfig.passthrough().ttl(DURATION_300S).build();
		QueryRuleSession ruleSession = QueryRuleSession.of(RulesetConfig.of(rule1, rule2));
		Action action = ruleSession.fire(BLAH_QUERY);
		Assertions.assertEquals(0, action.getTtl());
	}

	private static Query query(String sql, String... tables) {
		Query query = new Query();
		query.setId(String.valueOf(HashingFunctions.crc32(sql)));
		query.setSql(sql);
		query.setTables(new HashSet<>(Arrays.asList(tables)));
		return query;
	}

}
