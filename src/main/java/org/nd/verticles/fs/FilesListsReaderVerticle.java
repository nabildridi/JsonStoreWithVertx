package org.nd.verticles.fs;

import java.util.Optional;

import org.nd.managers.CachesManger;
import org.nd.routes.Routes;
import org.nd.verticles.rx.JsonArrayReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;

public class FilesListsReaderVerticle extends AbstractVerticle {
    private static Logger logger = LoggerFactory.getLogger(FilesListsReaderVerticle.class);

    public void start() {
	MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.READ_LIST_FILES);
	consumer.handler(message -> {
	    JsonArray keysArray = message.body();
	    JsonArray result = new JsonArray();

	    Flowable.fromIterable(keysArray).map(id -> String.valueOf(id))
		    .map(systemId -> CachesManger.jsonFromCache(systemId)).map(json -> Optional.of(json))
		    .reduce(result, new JsonArrayReducer()).subscribe(jsonArray -> message.reply(jsonArray));
	});
    }
}
