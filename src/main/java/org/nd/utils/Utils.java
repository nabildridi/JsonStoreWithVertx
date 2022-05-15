package org.nd.utils;

import java.util.Collections;
import java.util.List;

public class Utils {

	public static boolean notNullAndNotEmpty(String value) {
		
		return value!= null && !value.isEmpty() && !value.isBlank();
		
	}
	
	public static <T> List<T> getPage(List<T> sourceList, int page, int pageSize) {
	    
	    int fromIndex = page  * pageSize;
	    if(sourceList == null || sourceList.size() <= fromIndex){
	        return Collections.emptyList();
	    }
	    
	    // toIndex exclusive
	    return sourceList.subList(fromIndex, Math.min(fromIndex + pageSize, sourceList.size()));
	}
	
}
