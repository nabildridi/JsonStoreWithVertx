package org.nd.verticles.filtering;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;

public class SorterVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(SorterVerticle.class);

	public void start() {

		MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.SORTER);
		consumer.handler(message -> {
			logger.debug("Starting...");

			// Array to sort
			JsonArray keysArray = message.body();

			// queryHolder
			String jsonQueryStr = message.headers().get("jsonQuery");


			DeliveryOptions options = new DeliveryOptions().addHeader("jsonQuery", jsonQueryStr);

			vertx.eventBus().<JsonArray>request(Routes.GET_JSON_PATH_RESULT, keysArray, options, cf -> {

				JsonArray resultJsonObject = cf.result().body();

				message.reply(resultJsonObject);

			});

		});

	}

}
