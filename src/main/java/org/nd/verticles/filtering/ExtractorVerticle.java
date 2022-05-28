package org.nd.verticles.filtering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.nd.managers.CachesManger;
import org.nd.routes.Routes;
import org.nd.verticles.rx.JsonArrayReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.wnameless.json.unflattener.JsonUnflattener;

import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ExtractorVerticle extends AbstractVerticle {
    private static Logger logger = LoggerFactory.getLogger(ExtractorVerticle.class);

    // -----------------------------------------------------------------------------------------------------------------------------------------

    public void start() {

	// ------------------------------------------------------------------------------------------------------------------------
	MessageConsumer<Object> extractCconsumer = vertx.eventBus().consumer(Routes.EXTRACT);
	extractCconsumer.handler(message -> {

	    Object container = message.body();
	    String pathToExtract = message.headers().get("pathToExtract");

	    // fragments names
	    List<String> fragmentsNames = new ArrayList<String>();
	    try {
		String[] splits = pathToExtract.split(",");
		for (String split : splits) {
		    fragmentsNames.add(split.trim());
		}
	    } catch (Exception e) {
	    }

	    // if input is an array
	    if (container instanceof JsonArray) {

		JsonArray jsonsList = (JsonArray) container;
		JsonArray result = new JsonArray();

		Flowable.fromIterable(jsonsList)
		.map(object -> ((JsonObject) object).getString("_systemId"))
			.map(systemId -> Pair.of(CachesManger.flattenFromCache(systemId), systemId))
			.map(pair -> {

			    String systemId = pair.getRight();
			    Map<String, Object> flattenJson = pair.getLeft();
			    Map<String, Object> output = new HashMap<String, Object>();

			    for (String frName : fragmentsNames) {
				Object value = flattenJson.get(frName);
				if (value != null)
				    output.put(frName, value);
			    }
			    output.put("_systemId", systemId);
			    String outputJson = JsonUnflattener.unflatten(output);
			    Optional<JsonObject> extractResult = Optional.of(new JsonObject(outputJson));
			    return extractResult;

			})
			.reduce(result, new JsonArrayReducer())
			.subscribe(jsonArray -> {
			    message.reply(jsonArray);
			});

	    }

	    // if input is a jsonObject
	    if (container instanceof JsonObject) {

		JsonObject jo = (JsonObject) container;

		String systemId = jo.getString("_systemId");
		Map<String, Object> output = new HashMap<String, Object>();
		Map<String, Object> flattenJson = CachesManger.flattenFromCache(systemId);
		for (String frName : fragmentsNames) {
		    Object value = flattenJson.get(frName);
		    if (value != null)
			output.put(frName, value);
		}
		output.put("_systemId", systemId);

		String outputJson = JsonUnflattener.unflatten(output);

		message.reply(new JsonObject(outputJson));

	    }

	});

    }

}
