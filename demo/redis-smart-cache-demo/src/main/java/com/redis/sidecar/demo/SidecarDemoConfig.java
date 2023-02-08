package com.redis.sidecar.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redis.sidecar.BootstrapConfig;

@Configuration
@ConfigurationProperties(prefix = "")
public class SidecarDemoConfig {

	private BootstrapConfig sidecar = new BootstrapConfig();
	private DemoConfig demo = new DemoConfig();

	public BootstrapConfig getSidecar() {
		return sidecar;
	}

	public void setSidecar(BootstrapConfig sidecar) {
		this.sidecar = sidecar;
	}

	public DemoConfig getDemo() {
		return demo;
	}

	public void setDemo(DemoConfig demo) {
		this.demo = demo;
	}

	public static class DemoConfig {

		private boolean flush;
		private int queryThreads;
		private int batchSize;
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

		public int getBatchSize() {
			return batchSize;
		}

		public void setBatchSize(int batchSize) {
			this.batchSize = batchSize;
		}

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

	}

}
