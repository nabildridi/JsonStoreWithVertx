package org.nd.verticles.filtering;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.nd.managers.CachesManger;
import org.nd.routes.Routes;
import org.nd.verticles.rx.JsonArrayReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vavr.control.Try;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.shareddata.LocalMap;

public class FilterVerticle extends AbstractVerticle {
    private static Logger logger = LoggerFactory.getLogger(FilterVerticle.class);

    public void start() {

	MessageConsumer<String> consumer = vertx.eventBus().consumer(Routes.FILTER);
	consumer.handler(message -> {

	    LocalMap<String, String> filesMap = vertx.sharedData().getLocalMap("files");
	    List<String> keysList = List.copyOf(filesMap.keySet());

	    String JsonPathQuery = message.body();
	    JsonPath jsonPath = Try.of(() -> JsonPath.compile(JsonPathQuery)).getOrNull();
	    JsonArray result = new JsonArray();

	    Flowable.fromIterable(keysList).parallel().runOn(Schedulers.io()).map(id -> String.valueOf(id))
		    .map(systemId -> Pair.of(CachesManger.documentContextFromCache(systemId), systemId)).map(pair -> {

			Object results = Try.of(() -> pair.getLeft().read(jsonPath)).getOrNull();
			Optional<String> jsonPathResult = Optional.empty();
			if (results != null) {
			    if (results instanceof List) {
				if (!((List) results).isEmpty()) {
				    jsonPathResult = Optional.of(pair.getRight());
				}
			    } else {
				jsonPathResult = Optional.of(pair.getRight());
			    }
			}
			return jsonPathResult;

		    })
		    .sequential()
		   . filter(item -> item.isPresent())
		    .reduce(result, new JsonArrayReducer()).subscribe(jsonArray -> {
			message.reply(jsonArray);
		    });

	});

    }

}
