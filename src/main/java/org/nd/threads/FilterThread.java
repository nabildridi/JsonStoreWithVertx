package org.nd.threads;

import java.util.List;
import java.util.concurrent.Callable;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public class FilterThread implements Callable<String> {
	
	private String id;
	private JsonPath jsonPath;
	private DocumentContext dc;
		

	public FilterThread() {
		super();
	}

	public FilterThread(String id, JsonPath jsonPath, DocumentContext dc) {
		super();
		this.id = id;
		this.jsonPath = jsonPath;
		this.dc = dc;
	}



	@Override
	public String call() throws Exception {
		
		try {
			Object results = dc.read(jsonPath);
			if (results instanceof List) {
				List<Object> list = (List) results;
				if (list != null && list.size() > 0) {
					return id;
				} else {
					return null;
				}
			} else {
				if (results != null) {
					return id;
				} else {
					return null;
				}
			}
		} catch (Exception e) {
			return null;
		}
		
	}

}
