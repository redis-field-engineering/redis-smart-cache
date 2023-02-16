package com.redis.smartcache;

import java.sql.SQLException;

import javax.sql.RowSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.redis.smartcache.core.rowset.CachedRowSetFactory;
import com.redis.smartcache.core.rowset.CachedRowSetImpl;
import com.redis.smartcache.test.RowSetBuilder;

@TestInstance(Lifecycle.PER_CLASS)
class CachedRowSetTests {

	@Test
	void populate() throws SQLException {
		RowSetBuilder builder = RowSetBuilder.of(new CachedRowSetFactory());
		RowSet rowSet = builder.build();
		CachedRowSetImpl actual = new CachedRowSetImpl();
		actual.populate(rowSet);
		rowSet.beforeFirst();
		TestUtils.assertEquals(rowSet, actual);
	}

}
