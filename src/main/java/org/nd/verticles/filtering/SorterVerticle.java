package org.nd.verticles.filtering;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.nd.dto.QueryHolder;
import org.nd.managers.CachesManger;
import org.nd.routes.Routes;
import org.nd.utils.InverseComparator;
import org.nd.verticles.rx.SortedMapReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SorterVerticle extends AbstractVerticle {
    private static Logger logger = LoggerFactory.getLogger(SorterVerticle.class);

    public void start() {

	MessageConsumer<JsonArray> consumer = vertx.eventBus().consumer(Routes.SORTER);
	consumer.handler(message -> {

	    // Array to sort
	    JsonArray keysArray = message.body();

	    // queryHolder
	    String queryHolderStr = message.headers().get("queryHolder");
	    QueryHolder queryHolder = new JsonObject(queryHolderStr).mapTo(QueryHolder.class);

	    SortedMap<String, List<String>> resultMap = null;

	    if (queryHolder.getSortOrder().equals("1")) {
		resultMap = new TreeMap<>();
	    }
	    if (queryHolder.getSortOrder().equals("-1")) {
		resultMap = new TreeMap<>(new InverseComparator());
	    }

	    Flowable.fromIterable(keysArray).map(id -> String.valueOf(id))
		    .map(systemId -> Pair.of(CachesManger.flattenFromCache(systemId), systemId)).map(pair -> {

			String systemId = pair.getRight();
			Map<String, Object> flattenJson = pair.getLeft();
			Object result = flattenJson.get(queryHolder.getSortField());
			Pair<String, String> tuple = Pair.of(systemId, "");
			if (result != null) {
			    tuple = Pair.of(systemId, String.valueOf(result));
			}
			return tuple;
		    }).reduce(resultMap, new SortedMapReducer()).map(rm -> {

			List<String> sortedList = new LinkedList<String>();
			for (List<String> ids : rm.values()) {
			    sortedList.addAll(ids);
			}
			return new JsonArray(sortedList);
		    }).subscribe(jsonArray -> {
			message.reply(jsonArray);
		    });

	});

    }

}
