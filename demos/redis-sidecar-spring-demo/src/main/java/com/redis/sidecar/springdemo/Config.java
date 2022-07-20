package com.redis.sidecar.springdemo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "sidecar")
public class Config {

	private int rows;
	private int maxQuantity;
	private int batchSize;
	private int orderSpread;

	public int getOrderSpread() {
		return orderSpread;
	}

	public void setOrderSpread(int orderSpread) {
		this.orderSpread = orderSpread;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public int getMaxQuantity() {
		return maxQuantity;
	}

	public void setMaxQuantity(int maxQuantity) {
		this.maxQuantity = maxQuantity;
	}
}
