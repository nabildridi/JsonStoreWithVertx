package org.nd.verticles.rx;

import java.util.Optional;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.functions.BiFunction;
import io.vertx.core.json.JsonArray;

public class JsonArrayReducer implements BiFunction<JsonArray, Optional<?>, JsonArray> {

	@Override
	public @NonNull JsonArray apply(@NonNull JsonArray jsonArray, @NonNull Optional<?> value) throws Throwable {
		return jsonArray.add(value.get());
	}

}
