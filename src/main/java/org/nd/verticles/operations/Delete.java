package org.nd.verticles.operations;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

public class Delete extends AbstractVerticle {

	private static Logger logger = LoggerFactory.getLogger(Delete.class);

	public void start() {

		MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(Routes.DELETE);
		consumer.handler(message -> {

			JsonObject json = message.body();
			String id = message.headers().get("id");
			
			//get systemId
			String systemId = null;
			if(!id.isEmpty()) {
				systemId = id;
			}else {
				systemId = json.getString("_systemId");
			}
			
			//if systemId == null return with fail
			if(systemId == null) {
				message.fail(0, "systemId not found");
			}else {
				DeliveryOptions options = new DeliveryOptions();
				options.addHeader("systemId", systemId);
				vertx.eventBus().send(Routes.DELETE_FROM_FS, null, options);
				message.reply(systemId);
			}
			

		});

	}

}
