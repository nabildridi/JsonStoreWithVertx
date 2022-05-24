package org.nd.verticles.operations;

import org.nd.routes.Routes;
import org.nd.utils.TUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

public class SaveOrUpdate extends AbstractVerticle {

	private static Logger logger = LoggerFactory.getLogger(SaveOrUpdate.class);

	public void start() {

		MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(Routes.SAVE_OR_UPDATE);
		consumer.handler(message -> {

			JsonObject json = message.body();
			String systemId = null;
			DeliveryOptions options = new DeliveryOptions();

			// get id
			if (json.containsKey("_systemId")) {
				// it's an update
				systemId = json.getString("_systemId");
				options.addHeader("operation", "update");
			} else {
				// it's a new document
				systemId = (new TUID()).getId();
				json.put("_systemId", systemId);
				options.addHeader("operation", "save");
			}

			options.addHeader("systemId", systemId);
			vertx.eventBus().send(Routes.SAVE_TO_FS, json, options);
			message.reply(json);

		});

	}

}
