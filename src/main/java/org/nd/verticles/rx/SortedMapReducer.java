package org.nd.verticles.rx;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Multimap;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.functions.BiFunction;

public class SortedMapReducer
	implements BiFunction<Multimap<String, String>, Pair<String, String>, Multimap<String, String>> {

    @Override
    public @NonNull Multimap<String, String> apply(@NonNull Multimap<String, String> resultMap,
	    @NonNull Pair<String, String> pair) throws Throwable {
	// reverse : set key in value and value in key to sort by json value later
	resultMap.put(pair.getValue(), pair.getKey());

	return resultMap;
    }

}
