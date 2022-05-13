package org.nd.verticles.fs;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FilenameUtils;
import org.nd.dto.FileIdAndContent;
import org.nd.routes.Routes;
import org.nd.threads.FileReaderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.netty.util.concurrent.Future;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class FileSystemOperationsVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FileSystemOperationsVerticle.class);

	private String storeFolderPath;
	private LoadingCache<String, String> filesContentCache;
	private LoadingCache<String, JsonObject> jsonObjectCache;
	private LoadingCache<String, DocumentContext> documentContextCache;

	private Integer cachesSize;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);

		Instant start = Instant.now();

		// set cache size
		cachesSize = config().getInteger("cache_size");

		storeFolderPath = config().getString("store_fs_path");
		logger.debug("store folder path from the config file : " + storeFolderPath);

		// default value
		if (storeFolderPath == null) {
			String userHomeDir = System.getProperty("user.home");
			storeFolderPath = userHomeDir + File.separator + ".jsonStore";
		}

		// create json store folder is absent
		if (!vertx.fileSystem().existsBlocking(storeFolderPath)) {
			logger.debug("creating store folder in " + storeFolderPath);
			vertx.fileSystem().mkdirBlocking(storeFolderPath);
		}

		// construct files index
		List<String> filesnames = vertx.fileSystem().readDirBlocking(storeFolderPath);
		logger.debug("sourceDir files number :" + filesnames.size());

		List<String> ids = new ArrayList<String>();
		LocalMap<String, String> filesMap = vertx.sharedData().getLocalMap("files");
		for (String filename : filesnames) {
			String id = FilenameUtils.getBaseName(filename);
			filesMap.put(id, "");
			ids.add(id);
		}

		filesContentCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofMinutes(30))
				.expireAfterWrite(Duration.ofMinutes(30)).build(new CacheLoader<String, String>() {

					@Override
					public String load(String id) throws Exception {
						return getFromFs(id);
					}
				});

		jsonObjectCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofMinutes(30))
				.expireAfterWrite(Duration.ofMinutes(30)).build(new CacheLoader<String, JsonObject>() {

					@Override
					public JsonObject load(String id) throws Exception {
						return getJsonObject(id);
					}
				});

		documentContextCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofMinutes(30))
				.expireAfterWrite(Duration.ofMinutes(30)).build(new CacheLoader<String, DocumentContext>() {

					@Override
					public DocumentContext load(String id) throws Exception {
						return getDocumentContext(id);
					}
				});

		boolean makePreload = config().getBoolean("cache_preload", false);
		if (makePreload) {
			// initial preloading
			int preloadCount = Math.min(ids.size(), cachesSize);
			logger.debug("Preload activated, to preload " + preloadCount + " files, it may take some time");

			ExecutorService executor = Executors.newFixedThreadPool(16);
			CompletionService<FileIdAndContent> completionService = new ExecutorCompletionService<FileIdAndContent>(executor);


			for (int i = 0; i < preloadCount; i++) {
				String filename = ids.get(i) + ".json";
				String path = storeFolderPath + File.separator + filename;
				completionService.submit(new FileReaderThread(ids.get(i), path));
			}
			
			for (int i = 0; i < preloadCount; i++) {
				try {
					FileIdAndContent file = completionService.take().get();
					filesContentCache.put(file.getId(), file.getContent());
					jsonObjectCache.get(file.getId());
					documentContextCache.get(file.getId());
				} catch (Exception e) {} 
			}
			
			Instant end = Instant.now();
			Duration timeElapsed = Duration.between(start, end);
			logger.debug("intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");


		}

	}

	// -----------------------------------------------------------------------------------------------------------------------------------------

	private String getFromFs(String id) {

		String filename = id + ".json";
		String path = storeFolderPath + File.separator + filename;
		String content = vertx.fileSystem().readFileBlocking(path).toString();
		return content;
	}

	private JsonObject getJsonObject(String id) {
		try {
			return new JsonObject(filesContentCache.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	private DocumentContext getDocumentContext(String id) {
		try {
			return JsonPath.parse(filesContentCache.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	public void start(Promise<Void> startPromise) {

		MessageConsumer<String> consumer = vertx.eventBus().consumer(Routes.READ_FILE_TO_JSON);
		consumer.handler(message -> {

			String id = message.body();

			try {

				JsonObject jo = jsonObjectCache.get(id);
				if (jo != null) {
					message.reply(jo);
				} else {
					message.fail(1, "Json not found with id: " + id);
				}

			} catch (Exception e) {
				message.fail(2, "Error converting to json, file with id: " + id);
			}

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<String> toStringConsumer = vertx.eventBus().consumer(Routes.READ_FILE_TO_STRING);
		toStringConsumer.handler(message -> {

			String id = message.body();

			try {
				String content = filesContentCache.get(id);
				if (content != null) {
					message.reply(content);
				} else {
					message.fail(1, "File not found with id: " + id);
				}

			} catch (Exception e) {
				message.fail(2, "Error getting file content, file with id: " + id);
			}

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<String> jsonPathConsumer = vertx.eventBus().consumer(Routes.GET_JSON_PATH_RESULT);
		jsonPathConsumer.handler(message -> {

			String id = message.body();
			String JsonPathQuery = message.headers().get("JsonPathQuery");

			try {
				Object results = documentContextCache.get(id).read(JsonPathQuery);
				JsonObject result = null;
				if (results instanceof List) {
					List<Object> list = (List<Object>) results;
					result = new JsonObject().put("id", id).put("valueForSort", String.valueOf(list.get(0)));
				} else {
					result = new JsonObject().put("id", id).put("valueForSort", String.valueOf(results));
				}
				message.reply(result);
			} catch (Exception e) {
				JsonObject result = new JsonObject().put("id", id).put("valueForSort", null);
				message.reply(result);
			}

		});

		// ------------------------------------------------------------------------------------------------------------------------
		MessageConsumer<String> hasJsonPathConsumer = vertx.eventBus().consumer(Routes.HAS_JSON_PATH_RESULTS);
		hasJsonPathConsumer.handler(message -> {

			String id = message.body();
			String JsonPathQuery = message.headers().get("JsonPathQuery");

			try {
				Object results = documentContextCache.get(id).read(JsonPathQuery);
				if (results instanceof List) {
					List<Object> list = (List) results;
					if (list != null && list.size() > 0) {
						message.reply(true);
					} else {
						message.reply(false);
					}
				} else {
					if (results != null) {
						message.reply(true);
					} else {
						message.reply(false);
					}
				}
			} catch (Exception e) {
				message.reply(false);
			}

		});

		// ------------------------------------------------------------------------------------------------------------------------

		MessageConsumer<JsonObject> saveConsumer = vertx.eventBus().consumer(Routes.SAVE_TO_FS);
		saveConsumer.handler(message -> {
			logger.debug("saving to file sytem...");

			String systemId = message.headers().get("systemId");
			String operation = message.headers().get("operation");
			JsonObject json = message.body();

			String filename = systemId + ".json";
			String path = storeFolderPath + File.separator + filename;

			// Write in physical fs
			vertx.fileSystem().writeFile(path, Buffer.buffer(json.encode()), result -> {
				if (result.succeeded()) {

					// update in memory files list
					vertx.sharedData().getLocalMap("files").put(systemId, "");

					if (filesContentCache.getIfPresent(systemId) != null) {
						filesContentCache.invalidate(systemId);
					}
					if (jsonObjectCache.getIfPresent(systemId) != null) {
						jsonObjectCache.invalidate(systemId);
					}
					if (documentContextCache.getIfPresent(systemId) != null) {
						documentContextCache.invalidate(systemId);
					}

					filesContentCache.get(systemId);
					jsonObjectCache.get(systemId);
					documentContextCache.get(systemId);

					message.reply(true);
				} else {
					message.fail(0, "Error reading file: " + path.toString());
				}
			});

		});
		// -------------------------------------------------------------------------------------------------------------------

		MessageConsumer<JsonObject> deleteConsumer = vertx.eventBus().consumer(Routes.DELETE_FROM_FS);
		deleteConsumer.handler(message -> {
			logger.debug("deleteing from file sytem...");

			String systemId = message.headers().get("systemId");

			// remove the file
			String filename = systemId + ".json";
			String path = storeFolderPath + File.separator + filename;

			// delete from physical fs
			vertx.fileSystem().delete(path, result -> {
				if (result.succeeded()) {

					// update in memory files list
					vertx.sharedData().getLocalMap("files").remove(systemId);

					// remove from cache
					filesContentCache.invalidate(systemId);
					jsonObjectCache.invalidate(systemId);
					documentContextCache.invalidate(systemId);

					message.reply(true);
				} else {
					message.fail(0, "Error reading file: " + path.toString());
				}
			});

		});

		startPromise.complete();

	}

}
