package com.redis.sidecar.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ListResultSet extends AbstractResultSet {

	private final List<List<Object>> rows;
	private final AtomicInteger position = new AtomicInteger();

	public ListResultSet(CachedResultSetMetaData metaData, List<List<Object>> rows) {
		super(metaData);
		this.rows = rows;
	}

	@Override
	public boolean next() throws SQLException {
		return position.incrementAndGet() <= rows.size();
	}

	@Override
	public void close() throws SQLException {
		rows.clear();
		position.set(0);
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		position.set(row);
		return true;
	}

	@Override
	public void afterLast() throws SQLException {
		position.set(rows.size() + 1);
	}

	@Override
	public void beforeFirst() throws SQLException {
		position.set(0);
	}

	@Override
	public boolean first() throws SQLException {
		if (rows.isEmpty()) {
			return false;
		}
		position.set(1);
		return true;
	}

	@Override
	public boolean last() throws SQLException {
		if (rows.isEmpty()) {
			return false;
		}
		position.set(rows.size());
		return validRow();
	}

	private boolean validRow() {
		return position.get() > 0 && position.get() < rows.size() + 1;
	}

	@Override
	public boolean previous() throws SQLException {
		position.decrementAndGet();
		return validRow();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		position.addAndGet(rows);
		return validRow();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return !rows.isEmpty() && position.get() == 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return !rows.isEmpty() && position.get() == rows.size() + 1;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return position.get() == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		return position.get() == rows.size();
	}

	@Override
	public int getRow() throws SQLException {
		return position.get();
	}

	@Override
	protected List<Object> getCurrentRow() {
		return rows.get(position.get() - 1);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(rows);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ListResultSet other = (ListResultSet) obj;
		return Objects.equals(rows, other.rows);
	}

}
