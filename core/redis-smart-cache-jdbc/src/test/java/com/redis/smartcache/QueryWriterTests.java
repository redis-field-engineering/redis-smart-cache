package com.redis.smartcache;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.core.Config.AnalyzerConfig;
import com.redis.smartcache.core.KeyBuilder;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.QueryWriter;
import com.redis.testcontainers.RedisStackContainer;

@Testcontainers
class QueryWriterTests {

	@Container
	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Test
	void searchIndex() {
		String index = "query-idx";
		String keyspace = "query";
		KeyBuilder keyBuilder = new KeyBuilder(keyspace);
		AnalyzerConfig config = new AnalyzerConfig();
		RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
		try (StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			connection.setTimeout(Duration.ofMillis(100));
			String id = "123";
			String tables = "customers";
			try (QueryWriter writer = new QueryWriter(client, config, index, keyBuilder)) {
				Query query = new Query();
				query.setId(id);
				query.setSql("SELECT * FROM " + tables);
				query.setTables(new HashSet<>(Arrays.asList(tables)));
				writer.write(query);
				Awaitility.await().until(() -> connection.sync().ftSearch(index, "*").size() > 0);
			}
		}
	}

}
