package org.nd.verticles.operations;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hemantsonu20.json.JsonMerge;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

public class PartialUpdate extends AbstractVerticle {

	private static Logger logger = LoggerFactory.getLogger(PartialUpdate.class);

	public void start() {

		MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(Routes.PARTIAL_UPDATE);
		consumer.handler(message -> {
			logger.debug("Starting...");

			JsonObject partialJson = message.body();
			String systemId = partialJson.getString("_systemId");
			
		

			//if systemId == null return with fail
			if(systemId == null) {
				message.fail(0, "systemId not found");
			}else {
				vertx.eventBus().<JsonObject>request(Routes.READ_FILE_TO_JSON, systemId, ar -> {
					if (ar.succeeded()) {
						JsonObject mainDoc = ar.result().body();
						try {
							
							String mergeOutput = JsonMerge.merge(partialJson.encode(), mainDoc.encode());
							JsonObject mergedDoc = new JsonObject(mergeOutput);
							
							vertx.eventBus().request(Routes.SAVE_OR_UPDATE, mergedDoc, zr -> {
								if (zr.succeeded()) {
									message.reply(mergedDoc);
								}else {
									message.fail(2, "Error saving file");
								}
							});
							
						} catch (Exception e) {e.printStackTrace();message.fail(3, "Error patching file");}
						
						
					}else {
						message.fail(1, "Error reading file");
					}
				});
			}
			

		});

	}

}
