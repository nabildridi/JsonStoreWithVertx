package org.nd.verticles;

import org.nd.dto.QueryHolder;
import org.nd.routes.Routes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdista.util.Try;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.properties.PropertyFileAuthentication;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;

public class ServerVerticle extends AbstractVerticle {

	private static Logger logger = LoggerFactory.getLogger(ServerVerticle.class);

	private HttpServer server;
	private Router router;

	public void start() {

		server = vertx.createHttpServer();
		router = Router.router(vertx);
		router.route().handler(BodyHandler.create());
		EventBus eventBus = vertx.eventBus();

		router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

		// Simple auth service which uses a properties file for user/role info
		PropertyFileAuthentication authProvider = PropertyFileAuthentication.create(vertx, "vertx-users.properties");
		AuthenticationHandler basicAuthHandler = BasicAuthHandler.create(authProvider);

		router.route("/*").handler(basicAuthHandler);

		// get by id
		router.get("/:id").handler(ctx -> {

			String systemId = ctx.pathParam("id");
			QueryHolder queryHolder = new QueryHolder(ctx.queryParams());
			JsonObject queryJson = JsonObject.mapFrom(queryHolder);
			DeliveryOptions options = new DeliveryOptions().addHeader("systemId", systemId);

			HttpServerResponse response = ctx.response();
			eventBus.<JsonObject>request(Routes.GET_ONE, queryJson, options, ar -> {
				if (ar.succeeded()) {
					response.putHeader("content-type", "application/json");
					response.end(ar.result().body().encodePrettily());
				} else {
					response.setStatusCode(404).setStatusMessage("Id not found").end();
				}

			});

		});

		// query documents : pagination, sorting and filtering
		router.post("/query").consumes("*/json").handler(ctx ->

		{

			JsonObject query = Try.apply(() -> ctx.body().asJsonObject()).getOrElse(null);

			if (query != null) {

				// request query verticle
				eventBus.<JsonObject>request(Routes.QUERY, query, ar -> {
					HttpServerResponse response = ctx.response();
					if (ar.succeeded()) {
						response.putHeader("content-type", "application/json");
						response.end(ar.result().body().encode());
					} else {
						response.setStatusCode(400).setStatusMessage("Query uparseable").end();
					}
				});

			} else {

				HttpServerResponse response = ctx.response();
				response.setStatusCode(400).setStatusMessage("Unparsable json").end();
			}

		});

		// add or full update document
		router.post("/").consumes("*/json").handler(ctx -> {

			JsonObject json = Try.apply(() -> ctx.body().asJsonObject()).getOrElse(null);

			if (json != null) {

				eventBus.<JsonObject>request(Routes.SAVE_OR_UPDATE, json, ar -> {
					if (ar.succeeded()) {
						HttpServerResponse response = ctx.response();
						response.putHeader("content-type", "application/json");
						response.end(ar.result().body().encode());
					}
				});

			} else {

				HttpServerResponse response = ctx.response();
				response.setStatusCode(400).setStatusMessage("Unparsable json").end();
			}

		});

		// partial update
		router.put("/").handler(ctx -> {

			JsonObject partialJson = Try.apply(() -> ctx.body().asJsonObject()).getOrElse(null);

			// if json is invalid --> exit
			if (partialJson == null) {
				HttpServerResponse response = ctx.response();
				response.setStatusCode(400).setStatusMessage("Unparsable json").end();
			} else {

				// request to partial update verticle
				eventBus.<JsonObject>request(Routes.PARTIAL_UPDATE, partialJson, ar -> {
					if (ar.succeeded()) {
						HttpServerResponse response = ctx.response();
						response.putHeader("content-type", "application/json");
						response.end(ar.result().body().encode());
					} else {
						HttpServerResponse response = ctx.response();
						response.setStatusCode(400).setStatusMessage("systemId not found").end();
					}
				});

			}

		});

		// delete
		router.delete("/*").handler(ctx -> {

			String path = ctx.request().path();
			String[] pathFragments = path.substring(1).split("/");
			String id = pathFragments[0];

			JsonObject json = Try.apply(() -> ctx.body().asJsonObject()).getOrElse(null);

			// if json is invalid --> exit
			if (json == null && id.isEmpty()) {
				HttpServerResponse response = ctx.response();
				response.setStatusCode(400).end();
			} else {

				// request to delete verticle
				DeliveryOptions options = new DeliveryOptions();
				options.addHeader("id", id);

				eventBus.request(Routes.DELETE, null, options, ar -> {
					if (ar.succeeded()) {
						HttpServerResponse response = ctx.response();
						response.putHeader("content-type", "application/json");
						response.end(new JsonObject().put("_systemId", id).encode());
					} else {
						HttpServerResponse response = ctx.response();
						response.setStatusCode(400).setStatusMessage("systemId" + id + "not found").end();
					}
				});
			}

		});

		// get port from config
		Integer port = config().getInteger("application_port", 8080);
		server.requestHandler(router).listen(port, res -> {
		});
	}

	@Override
	public void stop() throws Exception {
		server.close();
	}

}
