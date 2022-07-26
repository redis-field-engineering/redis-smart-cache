package com.redis.sidecar.springdemo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "sidecar")
public class Config {

	private Loader loader = new Loader();
	private boolean flush;
	private int queryThreads;

	public int getQueryThreads() {
		return queryThreads;
	}

	public void setQueryThreads(int queryThreads) {
		this.queryThreads = queryThreads;
	}

	public boolean isFlush() {
		return flush;
	}

	public void setFlush(boolean flush) {
		this.flush = flush;
	}

	public Loader getLoader() {
		return loader;
	}

	public void setLoader(Loader loader) {
		this.loader = loader;
	}

	public static class Loader {

		private int batch;
		private int products;
		private int customers;
		private int orders;
		private int orderDetails;

		public int getBatch() {
			return batch;
		}

		public void setBatch(int batchSize) {
			this.batch = batchSize;
		}

		public int getProducts() {
			return products;
		}

		public void setProducts(int products) {
			this.products = products;
		}

		public int getCustomers() {
			return customers;
		}

		public void setCustomers(int customers) {
			this.customers = customers;
		}

		public int getOrders() {
			return orders;
		}

		public void setOrders(int orders) {
			this.orders = orders;
		}

		public int getOrderDetails() {
			return orderDetails;
		}

		public void setOrderDetails(int orderDetails) {
			this.orderDetails = orderDetails;
		}

	}

}
