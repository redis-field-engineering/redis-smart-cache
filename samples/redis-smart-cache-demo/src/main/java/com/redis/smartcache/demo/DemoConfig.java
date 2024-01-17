package com.redis.smartcache.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redis.smartcache.core.config.Config;

@Configuration
@ConfigurationProperties(prefix = "")
public class DemoConfig {

	private Config smartcache = new Config();
	private DataConfig demo = new DataConfig();

	public Config getSmartcache() {
		return smartcache;
	}

	public void setSmartcache(Config config) {
		this.smartcache = config;
	}

	public DataConfig getDemo() {
		return demo;
	}

	public void setDemo(DataConfig demo) {
		this.demo = demo;
	}

	public static class DataConfig {

		private boolean flush;
		private int threads;
		private int batch;
		private int customers;
		private int products;
		private int orders;
		private int orderdetails;

		public int getCustomers() {
			return customers;
		}

		public void setCustomers(int customers) {
			this.customers = customers;
		}

		public int getProducts() {
			return products;
		}

		public void setProducts(int products) {
			this.products = products;
		}

		public int getOrders() {
			return orders;
		}

		public void setOrders(int orders) {
			this.orders = orders;
		}

		public int getOrderdetails() {
			return orderdetails;
		}

		public void setOrderdetails(int orderdetails) {
			this.orderdetails = orderdetails;
		}

		public int getBatch() {
			return batch;
		}

		public void setBatch(int size) {
			this.batch = size;
		}

		public int getThreads() {
			return threads;
		}

		public void setThreads(int threads) {
			this.threads = threads;
		}

		public boolean isFlush() {
			return flush;
		}

		public void setFlush(boolean flush) {
			this.flush = flush;
		}

	}

}
