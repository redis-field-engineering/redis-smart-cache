package com.redis.sidecar.springdemo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "sidecar")
public class Config {

	private Loader loader = new Loader();
	private Query query = new Query();

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public Loader getLoader() {
		return loader;
	}

	public void setLoader(Loader loader) {
		this.loader = loader;
	}

	public static class Query {

		private int cardinality;
		private int results;
		private int threads;

		public int getThreads() {
			return threads;
		}

		public void setThreads(int threads) {
			this.threads = threads;
		}

		public int getCardinality() {
			return cardinality;
		}

		public void setCardinality(int cardinality) {
			this.cardinality = cardinality;
		}

		public int getResults() {
			return results;
		}

		public void setResults(int results) {
			this.results = results;
		}

	}

	public static class Loader {

		private int batch;
		private int rows;
		private boolean drop;

		public boolean isDrop() {
			return drop;
		}

		public void setDrop(boolean drop) {
			this.drop = drop;
		}

		public int getBatch() {
			return batch;
		}

		public void setBatch(int batchSize) {
			this.batch = batchSize;
		}

		public int getRows() {
			return rows;
		}

		public void setRows(int rows) {
			this.rows = rows;
		}

	}

}
