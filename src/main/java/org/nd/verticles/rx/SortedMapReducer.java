package org.nd.verticles.rx;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.lang3.tuple.Pair;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.functions.BiFunction;

public class SortedMapReducer
	implements BiFunction<SortedMap<String, List<String>>, Pair<String, String>, SortedMap<String, List<String>>> {

    @Override
    public @NonNull SortedMap<String, List<String>> apply(@NonNull SortedMap<String, List<String>> resultMap,
	    @NonNull Pair<String, String> pair) throws Throwable {
	String sortedMapKey = pair.getValue();
	if (!resultMap.containsKey(sortedMapKey)) {
	    resultMap.put(sortedMapKey, new ArrayList<String>());
	}
	resultMap.get(sortedMapKey).add(pair.getKey());
	return resultMap;
    }

}
