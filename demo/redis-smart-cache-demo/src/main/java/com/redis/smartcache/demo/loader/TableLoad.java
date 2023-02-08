package com.redis.smartcache.demo.loader;

public class TableLoad {

	private String table;
	private RowProvider rowProvider;
	private String[] columns;
	private int start;
	private int end;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public RowProvider getRowProvider() {
		return rowProvider;
	}

	public void setRowProvider(RowProvider rowProvider) {
		this.rowProvider = rowProvider;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String... columns) {
		this.columns = columns;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

}