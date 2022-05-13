package org.nd.verticles.fs;

import java.util.ArrayList;
import java.util.List;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class FilesListsReaderVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FilesListsReaderVerticle.class);

		public void start() {

		MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.READ_LIST_FILES);
		consumer.handler(message -> {

			JsonArray idsPage = message.body();
			
			List<Future> futures = new ArrayList<Future>();
			for (Object object : idsPage) {
				String id = (String) object;				
				futures.add(vertx.eventBus().request(Routes.READ_FILE_TO_JSON, id));
			}

			CompositeFuture.all(futures).onComplete(ar -> {

				if (ar.succeeded()) {
					
					JsonArray filesArray = new JsonArray();
					for (Future<Message<JsonObject>> future : futures) {
						if (future.succeeded()) {
							filesArray.add(future.result().body());
						}
					}
					
					message.reply(filesArray);

				} else {
					message.fail(0, "Error");
				}
			});

		});

	}

}
