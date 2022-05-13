package org.nd.verticles.filtering;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class FilterVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FilterVerticle.class);

	public void start() {

		MessageConsumer<String> consumer = vertx.eventBus().consumer(Routes.FILTER);
		consumer.handler(message -> {
			logger.debug("Starting...");

			String JsonPathQuery = message.body();

			LocalMap<String, String> filesMap = vertx.sharedData().getLocalMap("files");
			Set<String> idsSet = filesMap.keySet();

			// loop
			List<Future> futures = new ArrayList<Future>();
			DeliveryOptions options = new DeliveryOptions().addHeader("JsonPathQuery", JsonPathQuery);
			for(String idToCheck : idsSet) {
				futures.add(vertx.eventBus().request(Routes.CHECK, idToCheck , options));
			}

			JsonArray resultList = new JsonArray();
			CompositeFuture.all(futures).onComplete(cf -> {

				if (cf.succeeded()) {
					for (Future<Message<String>> future : futures) {
						if (future.succeeded()) {
							String id = future.result().body();
							if (id != null) {
								resultList.add(id);
							}
							
						}
					}
					message.reply(resultList);
				} else {
					message.reply(resultList);
				}

			});

	

		});

	}



}
