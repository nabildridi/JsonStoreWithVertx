package org.nd.verticles.filtering;

import java.util.List;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.shareddata.LocalMap;

public class FilterVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FilterVerticle.class);

	public void start() {

		MessageConsumer<String> consumer = vertx.eventBus().consumer(Routes.FILTER);
		consumer.handler(message -> {

			String JsonPathQuery = message.body();

			LocalMap<String, String> filesMap = vertx.sharedData().getLocalMap("files");
			
			List<String> keysList = List.copyOf(filesMap.keySet());
			JsonArray keysArray = new JsonArray(keysList);


			DeliveryOptions options = new DeliveryOptions().addHeader("JsonPathQuery", JsonPathQuery);
			
			
			vertx.eventBus().<JsonArray>request(Routes.CHECK, keysArray , options, cf -> {

				JsonArray resultList = new JsonArray();
				if (cf.succeeded()) {
					resultList = cf.result().body();
					message.reply(resultList);
				} else {
					message.reply(resultList);
				}

			});

	

		});

	}



}
