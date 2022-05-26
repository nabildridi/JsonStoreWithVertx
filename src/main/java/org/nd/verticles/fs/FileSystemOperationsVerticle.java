package org.nd.verticles.fs;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.nd.dto.QueryHolder;
import org.nd.managers.CachesManger;
import org.nd.managers.KvDatabaseManger;
import org.nd.routes.Routes;
import org.nd.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class FileSystemOperationsVerticle extends AbstractVerticle {
    private static Logger logger = LoggerFactory.getLogger(FileSystemOperationsVerticle.class);

    private LocalMap<String, String> filesMap;

    @Override
    public void init(Vertx vertx, Context context) {
	super.init(vertx, context);

	Instant start = Instant.now();
	filesMap = vertx.sharedData().getLocalMap("files");

	// init kv database
	KvDatabaseManger.init(config(), filesMap);

	// init caches
	List<String> ids = List.copyOf(filesMap.keySet());
	CachesManger.init(config(), ids);

	Instant end = Instant.now();
	Duration timeElapsed = Duration.between(start, end);
	logger.debug("intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");

    }

    // -----------------------------------------------------------------------------------------------------------------------------------------

    public void start(Promise<Void> startPromise) {

	// ------------------------------------------------------------------------------------------------------------------------

	MessageConsumer<JsonObject> getOneConsumer = vertx.eventBus().consumer(Routes.GET_ONE);
	getOneConsumer.handler(message -> {

	    String systemId = message.headers().get("systemId");
	    JsonObject jsonQuery = message.body();
	    QueryHolder queryHolder = jsonQuery.mapTo(QueryHolder.class);

	    JsonObject json = CachesManger.jsonFromCache(systemId);

	    if (json != null) {

		// if extarct path found try to extract element
		if (Utils.notNullAndNotEmpty(queryHolder.getExtract())) {

		    DeliveryOptions options = new DeliveryOptions().addHeader("pathToExtract",
			    queryHolder.getExtract());
		    vertx.eventBus().<JsonObject>request(Routes.EXTRACT, json, options, zr -> {
			JsonObject extracted = zr.result().body();
			message.reply(extracted);
		    });

		} else {
		    message.reply(json);
		}
	    } else {
		message.fail(0, "Error getting one : " + systemId);
	    }

	});

	// ------------------------------------------------------------------------------------------------------------------------

	MessageConsumer<JsonObject> saveConsumer = vertx.eventBus().consumer(Routes.SAVE_TO_FS);
	saveConsumer.handler(message -> {

	    String systemId = message.headers().get("systemId");
	    String operation = message.headers().get("operation");
	    JsonObject json = message.body();

	    try {

		// refresh kv database
		KvDatabaseManger.writeAndFlush(systemId, json.encode());
		// refresh cache
		CachesManger.invalidate(systemId, true);
		filesMap.put(systemId, "");

		message.reply(true);
	    } catch (Exception e) {
		e.printStackTrace();
		message.fail(0, "Error saving : " + systemId);
	    }

	});
	// -------------------------------------------------------------------------------------------------------------------

	MessageConsumer<JsonObject> deleteConsumer = vertx.eventBus().consumer(Routes.DELETE_FROM_FS);
	deleteConsumer.handler(message -> {

	    String systemId = message.headers().get("systemId");

	    try {

		// remove from kv database
		KvDatabaseManger.deleteAndFlush(systemId);

		filesMap.remove(systemId);

		// remove from cache
		CachesManger.invalidate(systemId, false);

		message.reply(true);

	    } catch (Exception e) {
		e.printStackTrace();
		message.fail(0, "Error removing : " + systemId);
	    }

	});

	startPromise.complete();

    }

}
