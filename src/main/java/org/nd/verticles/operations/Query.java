package org.nd.verticles.operations;

import java.util.List;

import org.nd.dto.QueryHolder;
import org.nd.routes.Routes;
import org.nd.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class Query extends AbstractVerticle {

	private static Logger logger = LoggerFactory.getLogger(Query.class);

	public void start() {

		MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(Routes.QUERY);
		consumer.handler(message -> {
			logger.debug("Starting...");

			JsonObject jsonQuery = message.body();
			QueryHolder queryHolder = jsonQuery.mapTo(QueryHolder.class);

			

			// if filter is null -> regular pagination
			if (!Utils.notNullAndNotEmpty( queryHolder.getFilter()) ) {
				
				LocalMap<String, String> filesMap = vertx.sharedData().getLocalMap("files");
				JsonArray allIdsJsonArray = new JsonArray( List.copyOf( filesMap.keySet()) );

				queryHolder.setTotalElement(allIdsJsonArray.size());
				
				//if files list is empty no need to continue
				if(allIdsJsonArray.size() == 0) {
					message.reply(constructJsonResponse(new JsonArray(), queryHolder));
					return;
				}

				makeSort(allIdsJsonArray, queryHolder).compose(ec -> makePagination(ec, queryHolder))
						.compose(sp -> makeData(sp)).compose(ec -> makeExtract(ec, queryHolder)).onComplete(ar -> {
							if (ar.succeeded()) {
								message.reply(constructJsonResponse(ar.result(), queryHolder));
							} else {
								message.reply(constructJsonResponse(new JsonArray(), queryHolder));
							}
						});

			}

			// if filter not null
			if (Utils.notNullAndNotEmpty( queryHolder.getFilter())) {

				logger.debug("filtering....");


				vertx.eventBus().<JsonArray>request(Routes.FILTER, queryHolder.getFilter(), at -> {
					if (at.succeeded()) {
						JsonArray filteredKeysResult = at.result().body();

						queryHolder.setTotalElement(filteredKeysResult.size());
						
						//if filtering result is empty no need to continue
						if(filteredKeysResult.size() == 0) {
							message.reply(constructJsonResponse(new JsonArray(), queryHolder));
							return;
						}					

						makeSort(filteredKeysResult, queryHolder).compose(ec -> makePagination(ec, queryHolder))
								.compose(sp -> makeData(sp)).compose(ec -> makeExtract(ec, queryHolder))
								.onComplete(ar -> {
									if (ar.succeeded()) {
										JsonArray filesData = (JsonArray) ar.result();
										message.reply(constructJsonResponse(filesData, queryHolder));
									} else {
										ar.cause().printStackTrace();
										message.reply(constructJsonResponse(new JsonArray(), queryHolder));
									}
								});

					} else {
						message.reply(constructJsonResponse(new JsonArray(), queryHolder));
					}
				});

			}

		});

	}

	private Future<JsonArray> makeSort(JsonArray unsortedKeysArray, QueryHolder queryHolder) {
		
		Promise<JsonArray> promise = Promise.promise();
		
		//does sorting info exists?
		if(!Utils.notNullAndNotEmpty(queryHolder.getSortField())  || !Utils.notNullAndNotEmpty(queryHolder.getSortOrder())) {			
			promise.complete(unsortedKeysArray);
			return promise.future();
		}
		
		//sort order must be "1" or "0"
		if(!queryHolder.getSortOrder().equals("1") && !queryHolder.getSortOrder().equals("-1")){
			promise.complete(unsortedKeysArray);
			return promise.future();
		}	
		
		//unsorted list must be not null or empty
		if(unsortedKeysArray == null || unsortedKeysArray.isEmpty()){
			promise.complete(unsortedKeysArray);
			return promise.future();
		}

		DeliveryOptions options = new DeliveryOptions();
		options.addHeader("jsonQuery", JsonObject.mapFrom(queryHolder).encode());

		vertx.eventBus().<JsonArray>request(Routes.SORTER, unsortedKeysArray, options, ar -> {

			if (ar.succeeded()) {
				JsonArray res = ar.result().body();
				promise.complete(res);
			} else {
				promise.complete(new JsonArray());
			}
		});
		return promise.future();
	}

	private Future<JsonArray> makePagination(JsonArray idsJson, QueryHolder queryHolder) {
		Promise<JsonArray> promise = Promise.promise();
		
		//page and size must be not null or negative
		if(queryHolder.getPage() == null || queryHolder.getPage()<0) {
			promise.complete(idsJson);
			return promise.future();
		}
		
		if(queryHolder.getSize() == null || queryHolder.getSize()<=0) {
			promise.complete(idsJson);
			return promise.future();
		}

		try {
			List<String> idsList = (List<String>) idsJson.getList();
			
			
			List<String> pagedList = Utils.getPage(idsList, queryHolder.getPage(), queryHolder.getSize());
		
			promise.complete(new JsonArray(pagedList));
		} catch (Exception e) {
			promise.complete(idsJson);
		}

		return promise.future();
	}

	private Future<JsonArray> makeData(JsonArray idsJson) {
		Promise<JsonArray> promise = Promise.promise();

		vertx.eventBus().<JsonArray>request(Routes.READ_LIST_FILES, idsJson, ar -> {
			if (ar.succeeded()) {
				JsonArray filesData = (JsonArray) ar.result().body();
				promise.complete(filesData);

			} else {
				promise.complete(new JsonArray());
			}
		});

		return promise.future();
	}

	private Future<JsonArray> makeExtract(JsonArray docs, QueryHolder queryHolder) {
		Promise<JsonArray> promise = Promise.promise();
		
		//does extract info exists?
		if(!Utils.notNullAndNotEmpty(queryHolder.getExtract()) ) {			
			promise.complete(docs);
			return promise.future();
		}

		DeliveryOptions options = new DeliveryOptions().addHeader("pathToExtract", queryHolder.getExtract());;

		vertx.eventBus().<JsonArray>request(Routes.EXTRACT, docs, options, zr -> {
			promise.complete(zr.result().body());
		});

		return promise.future();
	}

	private JsonObject constructJsonResponse(JsonArray docs, QueryHolder queryHolder) {
		JsonObject json = new JsonObject();

		json.put("content", docs);
		json.put("totalElements", queryHolder.getTotalElement());

		return json;
	}

}
