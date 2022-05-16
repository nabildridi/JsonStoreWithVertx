package org.nd.threads;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.github.wnameless.json.unflattener.JsonUnflattener;

import io.vertx.core.json.JsonObject;

public class ExtractThread implements Callable<JsonObject> {
	
	private String id;
	private List<String> fragmentsNames;
	private Map<String, Object> flattenJson;
		

	public ExtractThread() {
		super();
	}

	public ExtractThread(String id, List<String> fragmentsNames, Map<String, Object> flattenJson) {
		super();
		this.id = id;
		this.fragmentsNames = fragmentsNames;
		this.flattenJson = flattenJson;
	}



	@Override
	public JsonObject call() throws Exception {
		
		Map<String, Object> output = new HashMap<String, Object>();
		for (String frName : fragmentsNames) {
			Object value = flattenJson.get(frName);
			if (value != null)
				output.put(frName, value);
		}
		output.put("_systemId", id);
		String outputJson = JsonUnflattener.unflatten(output);
		return new JsonObject(outputJson);
		
	}

}
