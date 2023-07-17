package com.redis.smartcache.core;

import java.io.IOException;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.redis.smartcache.core.config.Config;

public class Mappers {

	public static final String PROPERTY_PREFIX = "smartcache";
	public static final String PROPERTY_PREFIX_DRIVER = PROPERTY_PREFIX + ".driver";
	public static final String PROPERTY_PREFIX_REDIS = PROPERTY_PREFIX + ".redis";

	private static final JavaPropsSchema PROPS_SCHEMA = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);
	private static final JavaPropsMapper PROPS_MAPPER = propsMapper();

	private Mappers() {
	}

	public static JavaPropsMapper propsMapper() {
		return JavaPropsMapper.builder().serializationInclusion(Include.NON_NULL)
				.propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE).build();
	}

	public static Properties properties(Config config) throws IOException {
		return PROPS_MAPPER.writeValueAsProperties(config, PROPS_SCHEMA);
	}

	public static Config config(Properties properties) throws IOException {
		return PROPS_MAPPER.readPropertiesAs(properties, PROPS_SCHEMA, Config.class);
	}

}
