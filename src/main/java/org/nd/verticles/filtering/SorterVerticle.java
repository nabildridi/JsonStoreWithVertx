package org.nd.verticles.filtering;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.nd.dto.QueryHolder;
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

public class SorterVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(SorterVerticle.class);

		public void start() {

		MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.SORTER);
		consumer.handler(message -> {
			logger.debug("Starting...");
			
			//Array to sort
			JsonArray keysArray = message.body();		
						
			//queryHolder
			String jsonQueryStr = message.headers().get("jsonQuery");
			JsonObject jsonQuery = new JsonObject(jsonQueryStr);
			QueryHolder queryHolder = jsonQuery.mapTo(QueryHolder.class);			

			
			Map<String, String> unSortedMap =new HashMap<String, String>();
			
			List<Future> futures = new ArrayList<Future>();
			
			DeliveryOptions options = new DeliveryOptions().addHeader("JsonPathQuery",  queryHolder.getSortField());	
			keysArray.forEach((id) -> {
				futures.add(vertx.eventBus().request(Routes.GET_JSON_PATH_RESULT, id, options));
			});
			
			CompositeFuture.all(futures).onComplete(cf -> {
				

					for (Future<Message<JsonObject>> future : futures) {
						JsonObject result = future.result().body();						
						unSortedMap.put(result.getString("id") , result.getString("valueForSort"));
					}
					
					//sort map
					LinkedHashMap<String, String> sortedMap = new LinkedHashMap<>();					
					
					if(queryHolder.getSortOrder().equals("1")) {
						unSortedMap.entrySet()
						  .stream()
						  .sorted(Map.Entry.comparingByValue())
						  .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
					}
					if(queryHolder.getSortOrder().equals("-1")) {
						unSortedMap.entrySet()
						  .stream()
						  .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) 
						  .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
					}
					
					List<String> sortedIdsList = List.copyOf( sortedMap.keySet());
					JsonArray res = new JsonArray(sortedIdsList); 		
					
					 message.reply(res);

			});
			

			 
		});

	}

}
