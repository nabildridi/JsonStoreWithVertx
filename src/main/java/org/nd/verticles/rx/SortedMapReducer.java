package org.nd.verticles.rx;

import java.util.SortedMap;

import org.apache.commons.lang3.tuple.Pair;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.functions.BiFunction;
import io.vertx.core.json.JsonArray;

public class SortedMapReducer
	implements BiFunction<SortedMap<String, JsonArray>, Pair<String, String>, SortedMap<String, JsonArray>> {

    @Override
    public @NonNull SortedMap<String, JsonArray> apply(@NonNull SortedMap<String, JsonArray> resultMap,
	    @NonNull Pair<String, String> pair) throws Throwable {
	String sortedMapKey = pair.getValue();
	if (!resultMap.containsKey(sortedMapKey)) {
	    resultMap.put(sortedMapKey, new JsonArray());
	}
	resultMap.get(sortedMapKey).add(pair.getKey());
	return resultMap;
    }

}
