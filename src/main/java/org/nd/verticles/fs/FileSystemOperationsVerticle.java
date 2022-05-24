package org.nd.verticles.fs;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.nd.routes.Routes;
import org.nd.utils.CachesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

public class FileSystemOperationsVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FileSystemOperationsVerticle.class);

	private String storeFolderPath;

	private LocalMap<String, String> filesMap;

	private SparkeyWriter kvWriter;
	private SparkeyReader kvReader;
	private File kvIndexFile = null;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);

		Instant start = Instant.now();

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
			vertx.fileSystem().mkdirsBlocking(storeFolderPath);
		}

		// init kv database
		kvIndexFile = new File(storeFolderPath + File.separator + "jsonStoreData.spi");

		try {
			if (!kvIndexFile.exists()) {
				logger.debug("Creating new file: " + kvIndexFile.getName());
				kvWriter = Sparkey.createNew(kvIndexFile);
				kvWriter.flush();
				kvWriter.writeHash();
			} else {
				kvWriter = Sparkey.append(kvIndexFile);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			kvReader = Sparkey.open(kvIndexFile);
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		// construct files index
		filesMap = vertx.sharedData().getLocalMap("files");
		try {
			for (SparkeyReader.Entry entry : kvReader) {
				String id = entry.getKeyAsString();
				filesMap.put(id, "");
			}
		} catch (Exception e1) {
		}

		logger.debug("Json documents number :" + filesMap.keySet().size());

		// init caches
		List<String> ids = List.copyOf(filesMap.keySet());
		CachesUtils.init(kvReader, config(), ids);

		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		logger.debug("intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");

	}

	// -----------------------------------------------------------------------------------------------------------------------------------------

	public void start(Promise<Void> startPromise) {

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

				// refresh cache
				CachesUtils.invalidate(systemId, true);

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
				CachesUtils.invalidate(systemId, false);

				message.reply(true);

			} catch (IOException e) {
				e.printStackTrace();
				message.fail(0, "Error removing : " + systemId);
			}

		});

		startPromise.complete();

	}

}
