package org.nd.verticles.jsonpath;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nd.routes.Routes;
import org.nd.threads.ExtractThread;
import org.nd.threads.FilterThread;
import org.nd.threads.SortValueGetterThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class JsonPathVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(JsonPathVerticle.class);

	private LoadingCache<String, DocumentContext> documentContextCache;
	private LoadingCache<String, Map<String, Object>> flattenCache;
	private LocalMap<String, String> filesMap;
	private Integer cachesSize;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);

		Instant start = Instant.now();

		// set cache size
		cachesSize = config().getInteger("cache_size");

		documentContextCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofDays(7))
				.build(new CacheLoader<String, DocumentContext>() {

					@Override
					public DocumentContext load(String id) throws Exception {
						return getDocumentContext(id);
					}
				});

		flattenCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofDays(7))
				.build(new CacheLoader<String, Map<String, Object>>() {

					@Override
					public Map<String, Object> load(String id) throws Exception {
						return getFlatten(id);
					}
				});

		// construct files index
		filesMap = vertx.sharedData().getLocalMap("files");
		List<String> ids = List.copyOf(filesMap.keySet());

		boolean makePreload = config().getBoolean("cache_preload", false);
		if (makePreload) {
			// initial preloading
			int preloadCount = Math.min(ids.size(), cachesSize);

			for (int i = 0; i < preloadCount; i++) {
				try {
					DocumentContext dc = getDocumentContext(ids.get(i));
					documentContextCache.put(ids.get(i), dc);

					Map<String, Object> fl = getFlatten(ids.get(i));
					flattenCache.put(ids.get(i), fl);

				} catch (Exception e) {
				}
			}

			Instant end = Instant.now();
			Duration timeElapsed = Duration.between(start, end);
			logger.debug("JsonPath intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");

		}

	}

	// -----------------------------------------------------------------------------------------------------------------------------------------

	private DocumentContext getDocumentContext(String id) {
		try {
			return JsonPath.parse(filesMap.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	private Map<String, Object> getFlatten(String id) {
		try {
			return JsonFlattener.flattenAsMap(filesMap.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	public void start(Promise<Void> startPromise) {

		MessageConsumer<JsonArray> jsonPathConsumer = vertx.eventBus().consumer(Routes.GET_JSON_PATH_RESULT);
		jsonPathConsumer.handler(message -> {

			JsonArray keysArray = message.body();
			String jsonPathQuery = message.headers().get("JsonPathQuery");

			ExecutorService executorService = Executors.newFixedThreadPool(64);
			CompletionService<JsonObject> executorCompletionService = new ExecutorCompletionService<JsonObject>(
					executorService);

			keysArray.forEach((id) -> {
				String systemId = (String) id;
				executorCompletionService
						.submit(new SortValueGetterThread(systemId, jsonPathQuery, flattenCache.get(systemId)));
			});

			JsonArray result = new JsonArray();
			for (int i = 0; i < keysArray.size(); i++) {
				try {
					result.add(executorCompletionService.take().get());
				} catch (Exception e) {
				}
			}
			
			try {
				executorService.shutdown();
			} catch (Exception e) {}

			message.reply(result);

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<JsonArray> hasJsonPathConsumer = vertx.eventBus().consumer(Routes.CHECK);
		hasJsonPathConsumer.handler(message -> {

			JsonArray keysArray = message.body();
			String jsonPathQuery = message.headers().get("JsonPathQuery");
			

			ExecutorService executorService = Executors.newFixedThreadPool(64);
			CompletionService<String> executorCompletionService = new ExecutorCompletionService<String>(
					executorService);

			keysArray.forEach((id) -> {
				String systemId = (String) id;
				executorCompletionService
						.submit(new FilterThread(systemId, jsonPathQuery, documentContextCache.get(systemId)));
			});
			

			JsonArray result = new JsonArray();
			for (int i = 0; i < keysArray.size(); i++) {
				try {
					String id = executorCompletionService.take().get();
					if(id!=null)result.add(id);
				} catch (Exception e) {
				}
			}
			
			try {
				executorService.shutdown();
			} catch (Exception e) {}

			message.reply(result);

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<String> JsonPathInvalidateConsumer = vertx.eventBus().consumer(Routes.JSON_PATH_INVALIDATE);
		JsonPathInvalidateConsumer.handler(message -> {

			String id = message.body();
			String reload = message.headers().get("needReoald");

			if (documentContextCache.getIfPresent(id) != null) {
				documentContextCache.invalidate(id);
			}

			if (flattenCache.getIfPresent(id) != null) {
				flattenCache.invalidate(id);
			}

			if (reload.equals("true")) {
				documentContextCache.get(id);
				flattenCache.get(id);
			}

			message.reply(true);

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<Object> extractCconsumer = vertx.eventBus().consumer(Routes.EXTRACT);
		extractCconsumer.handler(message -> {
			logger.debug("Starting...");

			Object container = message.body();
			String pathToExtract = message.headers().get("pathToExtract");

			// fragments names
			List<String> fragmentsNames = new ArrayList<String>();
			try {
				String[] splits = pathToExtract.split(",");
				for (String split : splits) {
					fragmentsNames.add(split.trim());
				}
			} catch (Exception e) {
			}

			// if input is an array
			if (container instanceof JsonArray) {

				JsonArray jsonsList = (JsonArray) container;
				ExecutorService executorService = Executors.newFixedThreadPool(64);
				CompletionService<JsonObject> executorCompletionService = new ExecutorCompletionService<JsonObject>(
						executorService);

				
				for (int i = 0; i < jsonsList.size(); i++) {
					JsonObject jo = jsonsList.getJsonObject(i);
					// get system id
					String systemId = jo.getString("_systemId");
					
					executorCompletionService
					.submit(new ExtractThread(systemId, fragmentsNames, flattenCache.get(systemId)));
				}
				
				
				JsonArray result = new JsonArray();
				for (int i = 0; i < jsonsList.size(); i++) {
					try {
						result.add(executorCompletionService.take().get());
					} catch (Exception e) {
					}
				}
				
				try {
					executorService.shutdown();
				} catch (Exception e) {}
				
				
				
				message.reply(result);
			}

			// if input is a jsonObject
			if (container instanceof JsonObject) {

				JsonObject jo = (JsonObject) container;

				String systemId = jo.getString("_systemId");
				Map<String, Object> output = new HashMap<String, Object>();
				for (String frName : fragmentsNames) {
					Object value = flattenCache.get(systemId).get(frName);
					if (value != null)
						output.put(frName, value);
				}
				output.put("_systemId", systemId);

				String outputJson = JsonUnflattener.unflatten(output);

				message.reply(new JsonObject(outputJson));

			}

		});

		startPromise.complete();

	}

}
