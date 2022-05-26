package org.nd.managers;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalComputable;

public class KvDatabaseManger {
	private static Logger logger = LoggerFactory.getLogger(KvDatabaseManger.class);

	private static Environment env;
	private static String storeName="json";
	private static Store store;
	private static  ExecutorService pool = Executors.newFixedThreadPool( 1 );

	public static void init(JsonObject config, LocalMap<String, String> filesMap) {
		Instant start = Instant.now();

		String storeFolderPath = config.getString("store_fs_path");
		logger.debug("store folder path from the config file : " + storeFolderPath);

		// default value
		if (storeFolderPath == null) {
			String userHomeDir = System.getProperty("user.home");
			storeFolderPath = userHomeDir + File.separator + ".jsonStore";
		}

		// create json store folder is absent
		File directory = new File(storeFolderPath);
		if (!directory.exists()) {
			logger.debug("creating store folder in " + storeFolderPath);
			directory.mkdirs();
		}


		env = Environments.newInstance(storeFolderPath);
				
		store = env.computeInTransaction(new TransactionalComputable<Store>() {
		    @Override
		    public Store compute(@NotNull final Transaction txn) {
		        return env.openStore(storeName, StoreConfig.WITHOUT_DUPLICATES, txn);
		    }
		});
		

		// construct files index		
        env.executeInTransaction(txn -> {
            try (Cursor cursor = store.openCursor(txn)) {
            	while (cursor.getNext()) {
            		String id = StringBinding.entryToString(cursor.getKey());
                    filesMap.put(id, "");
            	}
            }
        });
		


		 
		logger.debug("Json documents number :" + filesMap.keySet().size());

		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		logger.debug("KvDatabaseManger intialization completed; Time taken : " + timeElapsed.toSeconds() + " seconds");
	}

	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static String read(String systemId) {
		
		return  env.computeInReadonlyTransaction(new TransactionalComputable<String>() {
		    @Override
		    public String compute(@NotNull final Transaction txn) {
		    	ByteIterable v = store.get(txn, StringBinding.stringToEntry(systemId));
		    	return StringBinding.entryToString(v);
		    }
		});
		
	}

	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static void writeAndFlush(String systemId, String content) {
		 env.executeInTransaction(txn -> {
		        store.put(txn, StringBinding.stringToEntry(systemId), StringBinding.stringToEntry(content));
		 });
		
	}

	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static void deleteAndFlush(String systemId) {
		 env.executeInTransaction(txn -> {
		        store.delete(txn, StringBinding.stringToEntry(systemId));
		    });
	}

}
