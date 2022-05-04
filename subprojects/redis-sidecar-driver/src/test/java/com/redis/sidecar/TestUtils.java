package com.redis.sidecar;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;

public class TestUtils {

	public static void assertEquals(ResultSet expected, ResultSet actual) throws SQLException {
		ResultSetMetaData expectedMetaData = expected.getMetaData();
		ResultSetMetaData actualMetaData = actual.getMetaData();
		assertEquals(expectedMetaData, actualMetaData);
		int count = 0;
		while (expected.next()) {
			Assertions.assertTrue(actual.next());
			for (int index = 1; index <= expectedMetaData.getColumnCount(); index++) {
				Object expectedValue = expected.getObject(index);
				Object actualValue = actual.getObject(index);
				if (expectedValue == null) {
					Assertions.assertNull(actualValue);
				}
				Assertions.assertEquals(normalize(expectedValue), normalize(actualValue),
						String.format("Column %s type %s (%s)", expectedMetaData.getColumnName(index),
								expectedMetaData.getColumnTypeName(index), expectedMetaData.getColumnClassName(index)));
			}
			if (expected.getType() != ResultSet.TYPE_FORWARD_ONLY && actual.getType() != ResultSet.TYPE_FORWARD_ONLY) {
				Assertions.assertEquals(expected.isLast(), actual.isLast());
			}
			count++;
		}
		Assertions.assertTrue(count > 0);
	}

	private static Object normalize(Object value) {
		if (value instanceof BigDecimal) {
			return ((BigDecimal) value).doubleValue();
		}
		return value;
	}

	public static void assertEquals(ResultSetMetaData expectedMetaData, ResultSetMetaData actualMetaData)
			throws SQLException {
		Assertions.assertEquals(expectedMetaData.getColumnCount(), actualMetaData.getColumnCount());
		for (int index = 1; index <= expectedMetaData.getColumnCount(); index++) {
			Assertions.assertEquals(expectedMetaData.getColumnName(index), actualMetaData.getColumnName(index));
			Assertions.assertEquals(expectedMetaData.getColumnType(index), actualMetaData.getColumnType(index));
			Assertions.assertEquals(expectedMetaData.getColumnDisplaySize(index),
					actualMetaData.getColumnDisplaySize(index));
		}
	}
}
