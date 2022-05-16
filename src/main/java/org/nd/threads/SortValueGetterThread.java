package org.nd.threads;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Callable;

import io.vertx.core.json.JsonObject;

public class SortValueGetterThread implements Callable<Map.Entry<String, String>> {
	
	private String id;
	private String jsonPathQuery;
	private Map<String, Object> flattenJson;
		

	public SortValueGetterThread() {
		super();
	}

	public SortValueGetterThread(String id, String jsonPathQuery, Map<String, Object> flattenJson) {
		super();
		this.id = id;
		this.jsonPathQuery = jsonPathQuery;
		this.flattenJson = flattenJson;
	}



	@Override
	public Map.Entry<String, String> call() throws Exception {
		
		Map.Entry<String, String> tuple;
		try {
			Object result = flattenJson.get(jsonPathQuery);
			
			if (result != null) {
				tuple = new AbstractMap.SimpleEntry<>(id, String.valueOf(result));
				return tuple;
			} else {
				tuple = new AbstractMap.SimpleEntry<>(id, "");
				return tuple;
			}
		} catch (Exception e) {
			tuple = new AbstractMap.SimpleEntry<>(id, "");
			return tuple;
		}
		
	}

}
