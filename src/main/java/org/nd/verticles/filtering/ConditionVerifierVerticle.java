package org.nd.verticles.filtering;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;

public class ConditionVerifierVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(ConditionVerifierVerticle.class);

	public void start() {

		MessageConsumer<String> consumer = vertx.eventBus().consumer(Routes.CHECK);
		consumer.handler(message -> {

			// id
			String id = message.body();

			// JsonPathQuery
			String JsonPathQuery = message.headers().get("JsonPathQuery");
			DeliveryOptions options = new DeliveryOptions().addHeader("JsonPathQuery", JsonPathQuery);

			vertx.eventBus().<Boolean>request(Routes.HAS_JSON_PATH_RESULTS, id, options, at -> {
				if (at.succeeded()) {
					
					boolean found = at.result().body();
					
					if(found) {
						message.reply(id);
					}else {
						message.reply(null);
					}
					

				} else {
					message.reply(null);
				}
			});


		});

	}

}
