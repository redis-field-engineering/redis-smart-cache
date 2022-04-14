package com.redis.sidecar.impl;

import java.io.DataInputStream;
import java.io.IOException;

public class Column {

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

	public Column(DataInputStream input) throws IOException {
		catalog = input.readUTF();
		className = input.readUTF();
		label = input.readUTF();
		name = input.readUTF();
		typeName = input.readUTF();
		type = input.readInt();
		displaySize = input.readInt();
		precision = input.readInt();
		tableName = input.readUTF();
		scale = input.readInt();
		schemaName = input.readUTF();
		isAutoIncrement = input.readBoolean();
		isCaseSensitive = input.readBoolean();
		isCurrency = input.readBoolean();
		isDefinitelyWritable = input.readBoolean();
		isNullable = input.readInt();
		isReadOnly = input.readBoolean();
		isSearchable = input.readBoolean();
		isSigned = input.readBoolean();
		isWritable = input.readBoolean();
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getDisplaySize() {
		return displaySize;
	}

	public void setDisplaySize(int displaySize) {
		this.displaySize = displaySize;
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

}