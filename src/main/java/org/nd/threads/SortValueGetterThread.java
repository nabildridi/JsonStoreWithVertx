package org.nd.threads;

import java.util.Map;
import java.util.concurrent.Callable;

import io.vertx.core.json.JsonObject;

public class SortValueGetterThread implements Callable<JsonObject> {
	
	private String id;
	private String JsonPathQuery;
	private Map<String, Object> flattenJson;
		

	public SortValueGetterThread() {
		super();
	}

	public SortValueGetterThread(String id, String jsonPathQuery, Map<String, Object> flattenJson) {
		super();
		this.id = id;
		JsonPathQuery = jsonPathQuery;
		this.flattenJson = flattenJson;
	}



	@Override
	public JsonObject call() throws Exception {
		
		try {
			Object result = flattenJson.get(JsonPathQuery);

			if (result != null) {
				return new JsonObject().put("id", id).put("valueForSort", String.valueOf(result));
			} else {
				return new JsonObject().put("id", id).put("valueForSort", "");
			}
		} catch (Exception e) {
			return new JsonObject().put("id", id).put("valueForSort", "");
		}
		
	}

}
