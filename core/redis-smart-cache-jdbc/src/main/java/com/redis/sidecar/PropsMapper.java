package com.redis.sidecar;

import java.io.IOException;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class PropsMapper {

	public static final String PROPERTY_PREFIX = "sidecar";

	private final JavaPropsMapper mapper = JavaPropsMapper.builder()
			.propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE).serializationInclusion(Include.NON_NULL)
			.addModule(new JavaTimeModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
			.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).build();
	private final JavaPropsSchema schema = JavaPropsSchema.emptySchema().withPrefix(PROPERTY_PREFIX);

	public <T> T read(Properties info, Class<T> type) throws IOException {
		Properties properties = new Properties();
		properties.putAll(System.getenv());
		properties.putAll(System.getProperties());
		properties.putAll(info);
		return mapper.readPropertiesAs(properties, schema, type);
	}

	public Properties write(Object config) throws IOException {
		return mapper.writeValueAsProperties(config, schema);
	}

}
