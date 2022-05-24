package org.nd.utils;

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
import com.spotify.sparkey.SparkeyReader;

import io.vertx.core.json.JsonObject;

public class CachesUtils {
	private static Logger logger = LoggerFactory.getLogger(CachesUtils.class);

	private static LoadingCache<String, String> fileCache;
	private static LoadingCache<String, JsonObject> jsonObjectCache;
	private static LoadingCache<String, DocumentContext> documentContextCache;
	private static LoadingCache<String, Map<String, Object>> flattenCache;
	private static SparkeyReader kvReader;

	public static void init(SparkeyReader reader, JsonObject config, List<String> ids) {

		Instant start = Instant.now();

		kvReader = reader;
		Integer cachesSize = config.getInteger("cache_size");

		fileCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofDays(7))
				.build(new CacheLoader<String, String>() {

					@Override
					public String load(String id) throws Exception {
						return getFile(id);
					}
				});

		jsonObjectCache = Caffeine.newBuilder().maximumSize(cachesSize).expireAfterAccess(Duration.ofDays(7))
				.build(new CacheLoader<String, JsonObject>() {

					@Override
					public JsonObject load(String id) throws Exception {
						return getJsonObject(id);
					}
				});

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

	private static String getFile(String id) {
		try {
			return kvReader.getAsString(id);
		} catch (Exception e) {
			return null;
		}
	}

	private static JsonObject getJsonObject(String id) {
		try {
			return new JsonObject(getFile(id));
		} catch (Exception e) {
			return null;
		}
	}

	private static DocumentContext getDocumentContext(String id) {
		try {
			return JsonPath.parse(getFile(id));
		} catch (Exception e) {
			return null;
		}
	}

	private static Map<String, Object> getFlatten(String id) {
		try {
			return JsonFlattener.flattenAsMap(getFile(id));
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
			fileCache.get(id);
			jsonObjectCache.get(id);
			documentContextCache.get(id);
			flattenCache.get(id);
		}

	}
	// ------------------------------------------------------------------------------------------------------------------------


}
