package org.nd.verticles.fs;

import org.nd.routes.Routes;
import org.nd.utils.CachesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;

public class FilesListsReaderVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FilesListsReaderVerticle.class);

		public void start() {

		MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.READ_LIST_FILES);
		consumer.handler(message -> {

			JsonArray idsPage = message.body();
			
			JsonArray jsonsArray = new JsonArray();
			for (Object object : idsPage) {
				String id = (String) object;				
				jsonsArray.add(CachesUtils.jsonFromCache(id));
			}
			message.reply(jsonsArray);

		});

	}

}
