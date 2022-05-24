package org.nd.verticles.rx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.functions.BiFunction;

public class SortedMapReducer implements BiFunction<SortedMap<String, List<String>>, Map.Entry<String, String>, SortedMap<String, List<String>>> {

	@Override
	public @NonNull SortedMap<String, List<String>> apply(@NonNull SortedMap<String, List<String>> resultMap, @NonNull Map.Entry<String, String> entry)
			throws Throwable {
		String sortedMapKey = entry.getValue();
		if (!resultMap.containsKey(sortedMapKey)) {
			resultMap.put(sortedMapKey, new ArrayList<String>());
		}
		resultMap.get(sortedMapKey).add(entry.getKey());
		return resultMap;
	}

}
