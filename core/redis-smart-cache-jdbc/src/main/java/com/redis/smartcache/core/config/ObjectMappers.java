package com.redis.smartcache.core.config;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;

public class ObjectMappers {

	private ObjectMappers() {
	}

	public static JavaPropsMapper javaProps() {
		return JavaPropsMapper.builder().serializationInclusion(Include.NON_NULL)
				.propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
	}

	public static JsonMapper json() {
		return JsonMapper.builder().build();
	}

}
