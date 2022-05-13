package org.nd.dto;

import com.lambdista.util.Try;

import io.vertx.core.MultiMap;

public class QueryHolder {
	
	Integer page;
	Integer size;
	String sortField;
	String sortOrder;
	String filter;
	String extract;
	Integer totalElement;
	
			
	public QueryHolder() {
		super();
	}

	public QueryHolder(MultiMap queryMap) {

		extract = queryMap.get("extract");
		
	}
	

	public Integer getPage() {
		return page;
	}

	public void setPage(Integer page) {
		this.page = page;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getSortOrder() {
		return sortOrder;
	}
	public void setSortOrder(String sortOrder) {
		this.sortOrder = sortOrder;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}

	public Integer getTotalElement() {
		return totalElement;
	}

	public void setTotalElement(Integer totalElement) {
		this.totalElement = totalElement;
	}

	public String getExtract() {
		return extract;
	}

	public void setExtract(String extract) {
		this.extract = extract;
	}



}
