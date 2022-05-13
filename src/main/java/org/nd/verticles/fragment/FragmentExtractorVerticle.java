package org.nd.verticles.fragment;

import java.io.StringReader;
import java.io.StringWriter;

import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arakelian.json.ImmutableJsonFilterOptions;
import com.arakelian.json.ImmutableJsonFilterOptions.Builder;
import com.arakelian.json.JsonFilter;
import com.arakelian.json.JsonFilterOptions;
import com.arakelian.json.JsonReader;
import com.arakelian.json.JsonWriter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class FragmentExtractorVerticle extends AbstractVerticle {
	private static Logger logger = LoggerFactory.getLogger(FragmentExtractorVerticle.class);

	public void start() {

		MessageConsumer<Object> consumer = vertx.eventBus().consumer(Routes.EXTRACT);
		consumer.handler(message -> {
			logger.debug("Starting...");

			Object container = message.body();
			String pathToExtract = message.headers().get("pathToExtract");
			
			// configure filter
			Builder filtersBuilder = ImmutableJsonFilterOptions.builder();
			try {
				String[] splits = pathToExtract.split(",");
				for(String split : splits) {
					filtersBuilder.addIncludes(split.trim().replace(".", "/"));
				}
			} catch (Exception e) {}
			JsonFilterOptions jsonFilterOptions = filtersBuilder.build();
			

			// if input is an array
			if (container instanceof JsonArray) {

				JsonArray jsonsList = (JsonArray) container;
				JsonArray result = new JsonArray();
				for (int i = 0; i < jsonsList.size(); i++) {
					JsonObject jo = jsonsList.getJsonObject(i);

					// get system id
					String systemId = jo.getString("_systemId");

					JsonObject resultJson = process(jo.encode(), pathToExtract, systemId, jsonFilterOptions);
					result.add(resultJson);

				}
				message.reply(result);
			}

			// if input is a jsonObject
			if (container instanceof JsonObject) {

				JsonObject jo = (JsonObject) container;
				String systemId = jo.getString("_systemId");

				JsonObject resultJson = process(jo.encode(), pathToExtract, systemId, jsonFilterOptions);
				message.reply(resultJson);

			}

		});

	}

	private JsonObject process(String json, String pathToExtract, String systemId, JsonFilterOptions jsonFilterOptions) {

		// configure input and output streams
		JsonReader reader = new JsonReader(new StringReader(json));
		StringWriter sw = new StringWriter();
		JsonWriter<StringWriter> writer = new JsonWriter<StringWriter>(sw);

		

		JsonFilter filter = new JsonFilter(reader, writer, jsonFilterOptions);
		try {
			filter.process();
			JsonObject resultJson = new JsonObject(sw.toString());
			return resultJson.put("_systemId", systemId);
		} catch (Exception e) {
			e.printStackTrace();
			return new JsonObject().put("_systemId", systemId);
		}

	}

}
