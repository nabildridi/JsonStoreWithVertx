package org.nd.verticles.fs;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class FileSystemOperationsVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FileSystemOperationsVerticle.class);

	private String storeFolderPath;
	private LoadingCache<String, JsonObject> jsonObjectCache;

	private LocalMap<String, String> filesMap;
	private Integer cachesSize;

	private SparkeyWriter kvWriter;
	private SparkeyReader kvReader;
	private File kvIndexFile = null;

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

		// init kv database
		kvIndexFile = new File(storeFolderPath + File.separator + "jsonStoreData.spi");

		try {
			if (!kvIndexFile.exists()) {
				logger.debug("Creating new file: " + kvIndexFile.getName());
				kvWriter = Sparkey.createNew(kvIndexFile);
				kvWriter.flush();
				kvWriter.writeHash();
				// kvWriter.close();
			} else {
				kvWriter = Sparkey.append(kvIndexFile);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			kvReader = Sparkey.open(kvIndexFile);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		jsonObjectCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofDays(7))
				.build(new CacheLoader<String, JsonObject>() {

					@Override
					public JsonObject load(String id) throws Exception {
						return getJsonObject(id);
					}
				});

		// construct files index
		List<String> ids = new ArrayList<String>();
		filesMap = vertx.sharedData().getLocalMap("files");

		try {
			SparkeyReader reader = kvReader.duplicate();
			for (SparkeyReader.Entry entry : reader) {
				String id = entry.getKeyAsString();
				String content = entry.getValueAsString();
				filesMap.put(id, content);
				ids.add(id);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		logger.debug("Json documents number :" + ids.size());

		boolean makePreload = config().getBoolean("cache_preload", false);
		if (makePreload) {
			// initial preloading
			int preloadCount = Math.min(ids.size(), cachesSize);
			logger.debug("Preload activated, to preload " + preloadCount + " files, it may take some time");

			for (int i = 0; i < preloadCount; i++) {
				try {
					jsonObjectCache.get(ids.get(i));
				} catch (Exception e) {
				}
			}

			Instant end = Instant.now();
			Duration timeElapsed = Duration.between(start, end);
			logger.debug("intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");

		}

	}

	// -----------------------------------------------------------------------------------------------------------------------------------------

	private JsonObject getJsonObject(String id) {
		try {
			return new JsonObject(filesMap.get(id));
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

		MessageConsumer<JsonObject> saveConsumer = vertx.eventBus().consumer(Routes.SAVE_TO_FS);
		saveConsumer.handler(message -> {
			logger.debug("saving to file sytem...");

			String systemId = message.headers().get("systemId");
			String operation = message.headers().get("operation");
			JsonObject json = message.body();

			try {
				kvWriter.put(systemId, json.encode());
				kvWriter.flush();
				kvWriter.writeHash();

				if (filesMap.containsKey(systemId)) {
					filesMap.replace(systemId, json.encode());
				} else {
					filesMap.put(systemId, json.encode());
				}

				if (jsonObjectCache.getIfPresent(systemId) != null) {
					jsonObjectCache.invalidate(systemId);
				}

				jsonObjectCache.get(systemId);

				DeliveryOptions options = new DeliveryOptions().addHeader("needReoald", "true");
				vertx.eventBus().send(Routes.JSON_PATH_INVALIDATE, systemId, options);

				message.reply(true);
			} catch (IOException e) {
				e.printStackTrace();
				message.fail(0, "Error saving : " + systemId);
			}

		});
		// -------------------------------------------------------------------------------------------------------------------

		MessageConsumer<JsonObject> deleteConsumer = vertx.eventBus().consumer(Routes.DELETE_FROM_FS);
		deleteConsumer.handler(message -> {
			logger.debug("deleteing from file sytem...");

			String systemId = message.headers().get("systemId");

			try {
				kvWriter.delete(systemId);
				kvWriter.flush();
				kvWriter.writeHash();

				filesMap.remove(systemId);

				// remove from cache
				jsonObjectCache.invalidate(systemId);
				DeliveryOptions options = new DeliveryOptions().addHeader("needReoald", "false");
				vertx.eventBus().send(Routes.JSON_PATH_INVALIDATE, systemId, options);

				message.reply(true);

			} catch (IOException e) {
				e.printStackTrace();
				message.fail(0, "Error removing : " + systemId);
			}

		});

		startPromise.complete();

	}

}
