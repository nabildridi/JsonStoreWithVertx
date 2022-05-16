package org.nd.threads;

import java.util.List;
import java.util.concurrent.Callable;

import com.jayway.jsonpath.DocumentContext;

public class FilterThread implements Callable<String> {
	
	private String id;
	private String JsonPathQuery;
	private DocumentContext dc;
		

	public FilterThread() {
		super();
	}

	public FilterThread(String id, String jsonPathQuery, DocumentContext dc) {
		super();
		this.id = id;
		JsonPathQuery = jsonPathQuery;
		this.dc = dc;
	}



	@Override
	public String call() throws Exception {
		
		try {
			Object results = dc.read(JsonPathQuery);
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
