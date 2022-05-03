package com.redis.sidecar.impl;

import java.util.Objects;

public class ColumnMetaData {

	private String catalogName;
	private String columnClassName;
	private String columnLabel;
	private String columnName;
	private String columnTypeName;
	private int columnType;
	private int columnDisplaySize;
	private int precision;
	private String tableName;
	private int scale;
	private String schemaName;
	private boolean isAutoIncrement;
	private boolean isCaseSensitive;
	private boolean isCurrency;
	private boolean isDefinitelyWritable;
	private int isNullable;
	private boolean isReadOnly;
	private boolean isSearchable;
	private boolean isSigned;
	private boolean isWritable;

	public ColumnMetaData() {
	}

	private ColumnMetaData(Builder builder) {
		this.catalogName = builder.catalog;
		this.columnClassName = builder.className;
		this.columnLabel = builder.label;
		this.columnName = builder.name;
		this.columnTypeName = builder.typeName;
		this.columnType = builder.type;
		this.columnDisplaySize = builder.displaySize;
		this.precision = builder.precision;
		this.tableName = builder.tableName;
		this.scale = builder.scale;
		this.schemaName = builder.schemaName;
		this.isAutoIncrement = builder.isAutoIncrement;
		this.isCaseSensitive = builder.isCaseSensitive;
		this.isCurrency = builder.isCurrency;
		this.isDefinitelyWritable = builder.isDefinitelyWritable;
		this.isNullable = builder.isNullable;
		this.isReadOnly = builder.isReadOnly;
		this.isSearchable = builder.isSearchable;
		this.isSigned = builder.isSigned;
		this.isWritable = builder.isWritable;
	}

	public String getCatalogName() {
		return catalogName;
	}

	public void setCatalogName(String catalog) {
		this.catalogName = catalog;
	}

	public String getColumnClassName() {
		return columnClassName;
	}

	public void setColumnClassName(String className) {
		this.columnClassName = className;
	}

	public String getColumnLabel() {
		return columnLabel;
	}

	public void setColumnLabel(String label) {
		this.columnLabel = label;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String name) {
		this.columnName = name;
	}

	public String getColumnTypeName() {
		return columnTypeName;
	}

	public void setColumnTypeName(String typeName) {
		this.columnTypeName = typeName;
	}

	public int getColumnType() {
		return columnType;
	}

	public void setColumnType(int type) {
		this.columnType = type;
	}

	public int getColumnDisplaySize() {
		return columnDisplaySize;
	}

	public void setColumnDisplaySize(int displaySize) {
		this.columnDisplaySize = displaySize;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public boolean isAutoIncrement() {
		return isAutoIncrement;
	}

	public void setAutoIncrement(boolean isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}

	public boolean isCaseSensitive() {
		return isCaseSensitive;
	}

	public void setCaseSensitive(boolean isCaseSensitive) {
		this.isCaseSensitive = isCaseSensitive;
	}

	public boolean isCurrency() {
		return isCurrency;
	}

	public void setCurrency(boolean isCurrency) {
		this.isCurrency = isCurrency;
	}

	public boolean isDefinitelyWritable() {
		return isDefinitelyWritable;
	}

	public void setDefinitelyWritable(boolean isDefinitelyWritable) {
		this.isDefinitelyWritable = isDefinitelyWritable;
	}

	public int getIsNullable() {
		return isNullable;
	}

	public void setIsNullable(int isNullable) {
		this.isNullable = isNullable;
	}

	public boolean isReadOnly() {
		return isReadOnly;
	}

	public void setReadOnly(boolean isReadOnly) {
		this.isReadOnly = isReadOnly;
	}

	public boolean isSearchable() {
		return isSearchable;
	}

	public void setSearchable(boolean isSearchable) {
		this.isSearchable = isSearchable;
	}

	public boolean isSigned() {
		return isSigned;
	}

	public void setSigned(boolean isSigned) {
		this.isSigned = isSigned;
	}

	public boolean isWritable() {
		return isWritable;
	}

	public void setWritable(boolean isWritable) {
		this.isWritable = isWritable;
	}

	@Override
	public int hashCode() {
		return Objects.hash(catalogName, columnClassName, columnDisplaySize, isAutoIncrement, isCaseSensitive, isCurrency,
				isDefinitelyWritable, isNullable, isReadOnly, isSearchable, isSigned, isWritable, columnLabel, columnName,
				precision, scale, schemaName, tableName, columnType, columnTypeName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnMetaData other = (ColumnMetaData) obj;
		return Objects.equals(catalogName, other.catalogName) && Objects.equals(columnClassName, other.columnClassName)
				&& columnDisplaySize == other.columnDisplaySize && isAutoIncrement == other.isAutoIncrement
				&& isCaseSensitive == other.isCaseSensitive && isCurrency == other.isCurrency
				&& isDefinitelyWritable == other.isDefinitelyWritable && isNullable == other.isNullable
				&& isReadOnly == other.isReadOnly && isSearchable == other.isSearchable && isSigned == other.isSigned
				&& isWritable == other.isWritable && Objects.equals(columnLabel, other.columnLabel)
				&& Objects.equals(columnName, other.columnName) && precision == other.precision && scale == other.scale
				&& Objects.equals(schemaName, other.schemaName) && Objects.equals(tableName, other.tableName)
				&& columnType == other.columnType && Objects.equals(columnTypeName, other.columnTypeName);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private String catalog;
		private String className;
		private String label;
		private String name;
		private String typeName;
		private int type;
		private int displaySize;
		private int precision;
		private String tableName;
		private int scale;
		private String schemaName;
		private boolean isAutoIncrement;
		private boolean isCaseSensitive;
		private boolean isCurrency;
		private boolean isDefinitelyWritable;
		private int isNullable;
		private boolean isReadOnly;
		private boolean isSearchable;
		private boolean isSigned;
		private boolean isWritable;

		private Builder() {
		}

		public Builder catalog(String catalog) {
			this.catalog = catalog;
			return this;
		}

		public Builder className(String className) {
			this.className = className;
			return this;
		}

		public Builder label(String label) {
			this.label = label;
			return this;
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder typeName(String typeName) {
			this.typeName = typeName;
			return this;
		}

		public Builder type(int type) {
			this.type = type;
			return this;
		}

		public Builder displaySize(int displaySize) {
			this.displaySize = displaySize;
			return this;
		}

		public Builder precision(int precision) {
			this.precision = precision;
			return this;
		}

		public Builder tableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder scale(int scale) {
			this.scale = scale;
			return this;
		}

		public Builder schemaName(String schemaName) {
			this.schemaName = schemaName;
			return this;
		}

		public Builder isAutoIncrement(boolean isAutoIncrement) {
			this.isAutoIncrement = isAutoIncrement;
			return this;
		}

		public Builder isCaseSensitive(boolean isCaseSensitive) {
			this.isCaseSensitive = isCaseSensitive;
			return this;
		}

		public Builder isCurrency(boolean isCurrency) {
			this.isCurrency = isCurrency;
			return this;
		}

		public Builder isDefinitelyWritable(boolean isDefinitelyWritable) {
			this.isDefinitelyWritable = isDefinitelyWritable;
			return this;
		}

		public Builder isNullable(int isNullable) {
			this.isNullable = isNullable;
			return this;
		}

		public Builder isReadOnly(boolean isReadOnly) {
			this.isReadOnly = isReadOnly;
			return this;
		}

		public Builder isSearchable(boolean isSearchable) {
			this.isSearchable = isSearchable;
			return this;
		}

		public Builder isSigned(boolean isSigned) {
			this.isSigned = isSigned;
			return this;
		}

		public Builder isWritable(boolean isWritable) {
			this.isWritable = isWritable;
			return this;
		}

		public ColumnMetaData build() {
			return new ColumnMetaData(this);
		}
	}

}