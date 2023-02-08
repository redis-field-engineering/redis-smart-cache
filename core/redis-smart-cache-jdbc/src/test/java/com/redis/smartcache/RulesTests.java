package com.redis.smartcache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.RulesetConfig.RuleConfig;

class RulesTests {

	private static final String PRODUCT = "product";
	private static final String CUSTOMER = "customer";
	private static final String ORDER = "order";

	@Test
	void testTables() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tables(PRODUCT, CUSTOMER, ORDER).ttl(ttl).build();
		StatementRuleSession ruleSession = StatementRuleSession.of(RulesetConfig.of(rule));
		SmartCacheStatement statement = statement(ttl, PRODUCT, CUSTOMER, ORDER);
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = statement(SmartCacheStatement.TTL_NO_CACHE, PRODUCT, CUSTOMER);
		ruleSession.fire(statement);
		Assertions.assertEquals(SmartCacheStatement.TTL_NO_CACHE, statement.getTtl());
	}

	@Test
	void testTablesAny() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tablesAny(PRODUCT, CUSTOMER).ttl(ttl).build();
		StatementRuleSession ruleSession = StatementRuleSession.of(RulesetConfig.of(rule));
		SmartCacheStatement statement = statement(ttl, PRODUCT);
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = statement(SmartCacheStatement.TTL_NO_CACHE, ORDER);
		ruleSession.fire(statement);
		Assertions.assertEquals(SmartCacheStatement.TTL_NO_CACHE, statement.getTtl());
	}

	@Test
	void testTablesAll() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.tablesAll(PRODUCT, CUSTOMER).ttl(ttl).build();
		StatementRuleSession ruleSession = StatementRuleSession.of(RulesetConfig.of(rule));
		SmartCacheStatement statement = statement(ttl, PRODUCT, CUSTOMER, ORDER);
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = statement(SmartCacheStatement.TTL_NO_CACHE, ORDER);
		ruleSession.fire(statement);
		Assertions.assertEquals(SmartCacheStatement.TTL_NO_CACHE, statement.getTtl());
	}

	@Test
	void testRegex() {
		long ttl = 123;
		RuleConfig rule = RuleConfig.regex("SELECT\\s+\\*\\s+FROM\\s+.*").ttl(ttl).build();
		StatementRuleSession ruleSession = StatementRuleSession.of(RulesetConfig.of(rule));
		SmartCacheStatement statement = new SmartCacheStatement(null, null);
		statement.setSql("SELECT * FROM blah");
		ruleSession.fire(statement);
		Assertions.assertEquals(ttl, statement.getTtl());
		statement = new SmartCacheStatement(null, null);
		statement.setSql("SELECT COUNT(*) FROM blah");
		ruleSession.fire(statement);
		Assertions.assertEquals(SmartCacheStatement.TTL_NO_CACHE, statement.getTtl());
	}

	private SmartCacheStatement statement(long ttl, String... tables) {
		SmartCacheStatement statement = new SmartCacheStatement(null, null);
		statement.setTtl(ttl);
		statement.setTableNames(tables);
		return statement;
	}

}
