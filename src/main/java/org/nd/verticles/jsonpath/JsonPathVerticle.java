package org.nd.verticles.jsonpath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.nd.routes.Routes;
import org.nd.utils.CachesUtils;
import org.nd.verticles.rx.JsonArrayReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonPathVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(JsonPathVerticle.class);

	// -----------------------------------------------------------------------------------------------------------------------------------------

	public void start() {

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<JsonArray> hasJsonPathConsumer = vertx.eventBus().consumer(Routes.CHECK);
		hasJsonPathConsumer.handler(message -> {

			JsonArray keysArray = message.body();
			String jsonPathQuery = message.headers().get("JsonPathQuery");
			JsonPath jsonPath = JsonPath.compile(jsonPathQuery);
			JsonArray result = new JsonArray();

			Observable.just(keysArray).flatMapIterable(id -> id).map(id -> {

				String systemId = (String) id;
				DocumentContext dc = CachesUtils.documentContextFromCache(systemId);
				Object results = dc.read(jsonPath);
				Optional<String> jsonPathResult = Optional.empty();
				if (results != null) {
					if (results instanceof List) {
						if ( !((List)results).isEmpty()) {
							jsonPathResult = Optional.of(systemId);
						}
					} else {

						jsonPathResult = Optional.of(systemId);

					}
				}
				return jsonPathResult;

			}).reduce(result, new JsonArrayReducer()).subscribe(jsonArray -> {
				message.reply(jsonArray);
			});

		});

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

				Observable.just(jsonsList).flatMapIterable(object -> object).map(object -> {

					JsonObject jo = (JsonObject) object;
					String systemId = jo.getString("_systemId");
					Map<String, Object> flattenJson = CachesUtils.flattenFromCache(systemId);
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

				}).reduce(result, new JsonArrayReducer()).subscribe(jsonArray -> {
					message.reply(jsonArray);
				});

			}

			// if input is a jsonObject
			if (container instanceof JsonObject) {

				JsonObject jo = (JsonObject) container;

				String systemId = jo.getString("_systemId");
				Map<String, Object> output = new HashMap<String, Object>();
				for (String frName : fragmentsNames) {
					Object value = CachesUtils.flattenFromCache(systemId).get(frName);
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
