package org.nd.managers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.vertx.core.json.JsonObject;

public class CachesManger {
	private static Logger logger = LoggerFactory.getLogger(CachesManger.class);

	private static LoadingCache<String, String> fileCache;
	private static LoadingCache<String, JsonObject> jsonObjectCache;
	private static LoadingCache<String, DocumentContext> documentContextCache;
	private static LoadingCache<String, Map<String, Object>> flattenCache;

	public static void init(JsonObject config, List<String> ids) {

		Instant start = Instant.now();

		Integer cachesSize = config.getInteger("cache_size");

		fileCache = Caffeine.newBuilder().maximumSize(cachesSize).build(key -> getFile(key));

		jsonObjectCache = Caffeine.newBuilder().maximumSize(cachesSize).build(key -> getJsonObject(key));

		documentContextCache = Caffeine.newBuilder().maximumSize(cachesSize).build(key -> getDocumentContext(key));

		flattenCache = Caffeine.newBuilder().maximumSize(cachesSize).build(key -> getFlatten(key));
				

		boolean makePreload = config.getBoolean("cache_preload", false);
		if (makePreload) {
			// initial preloading
			int preloadCount = Math.min(ids.size(), cachesSize);
			logger.debug("Preload activated, to preload " + preloadCount + " files, it may take some time");

			for (int i = 0; i < preloadCount; i++) {
				String id = ids.get(i);
				try {
					fileCache.put(id, getFile(id));
					jsonObjectCache.put(id, getJsonObject(id));
					documentContextCache.put(id, getDocumentContext(id));
					flattenCache.put(id, getFlatten(id));
				} catch (Exception e) {
				}
			}

			Instant end = Instant.now();
			Duration timeElapsed = Duration.between(start, end);
			logger.debug("Caches preload completed; Time taken : " + timeElapsed.toSeconds() + " seconds");

		}

	}

	// -----------------------------------------------------------------------------------------------------------------------------------------

	// -----------------------------------------------------------------------------------------------------------------------------------------

	private static String getFile(String id) {
		return KvDatabaseManger.read(id);
	}

	private static JsonObject getJsonObject(String id) {
		try {
			return new JsonObject(fileCache.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	private static DocumentContext getDocumentContext(String id) {
		try {
			return JsonPath.parse(fileCache.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	private static Map<String, Object> getFlatten(String id) {
		try {
			return JsonFlattener.flattenAsMap(fileCache.get(id));
		} catch (Exception e) {
			return null;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static String stringFromCache(String id) {
		try {

			return fileCache.get(id);
		} catch (Exception e) {
			return null;
		}

	}

	// ------------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static JsonObject jsonFromCache(String id) {
		try {

			return jsonObjectCache.get(id);
		} catch (Exception e) {
			return null;
		}

	}

	// ------------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static Map<String, Object> flattenFromCache(String id) {
		try {

			return flattenCache.get(id);
		} catch (Exception e) {
			return null;
		}

	}

	// ------------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static DocumentContext documentContextFromCache(String id) {
		try {

			return documentContextCache.get(id);
		} catch (Exception e) {
			return null;
		}

	}

	// ------------------------------------------------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------------------------------------------------------------
	public static void invalidate(String id, boolean reload) {
		if (fileCache.getIfPresent(id) != null) {
			fileCache.invalidate(id);
		}

		if (jsonObjectCache.getIfPresent(id) != null) {
			jsonObjectCache.invalidate(id);
		}

		if (documentContextCache.getIfPresent(id) != null) {
			documentContextCache.invalidate(id);
		}

		if (flattenCache.getIfPresent(id) != null) {
			flattenCache.invalidate(id);
		}

		if (reload) {
			fileCache.put(id, getFile(id));
			jsonObjectCache.put(id, getJsonObject(id));
			documentContextCache.put(id, getDocumentContext(id));
			flattenCache.put(id, getFlatten(id));
		}

	}
	// ------------------------------------------------------------------------------------------------------------------------
	
}