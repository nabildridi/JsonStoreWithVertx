package org.nd.verticles.filtering;

import java.util.Comparator;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.nd.dto.QueryHolder;
import org.nd.managers.CachesManger;
import org.nd.routes.Routes;
import org.nd.utils.InverseComparator;
import org.nd.verticles.rx.SortedMapReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import io.reactivex.rxjava3.core.Flowable;
import io.vavr.control.Try;
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

	    Multimap<String, String> resultMap = null;
	    Comparator<String> natural = Comparator.<String>naturalOrder();
	    
	    if (queryHolder.getSortOrder().equals("1")) {
		resultMap = TreeMultimap.create();
	    }
	    if (queryHolder.getSortOrder().equals("-1")) {
		resultMap = TreeMultimap.create(new InverseComparator(), natural);
	    }

	    Flowable
	    .fromIterable(keysArray)
	    .map(id -> String.valueOf(id))
		    .map(systemId -> Pair.of(CachesManger.flattenFromCache(systemId), systemId))
		    .map(pair -> {

			String systemId = pair.getRight();
			Map<String, Object> flattenJson = pair.getLeft();			
			String result = Try.of(() -> String.valueOf( flattenJson.get(queryHolder.getSortField())) ).getOrElse("");
			return Pair.of(systemId, result);
		    })
		    .reduce(resultMap, new SortedMapReducer())
		    .subscribe(sortedMap -> {
			
			JsonArray jsonArray = new JsonArray();			
			sortedMap.values().forEach(item -> jsonArray.add(item));
			message.reply( jsonArray);
			
		    });

	});

    }

}
