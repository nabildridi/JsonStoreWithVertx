package org.nd;

import org.nd.verticles.ServerVerticle;
import org.nd.verticles.filtering.ConditionVerifierVerticle;
import org.nd.verticles.filtering.FilterVerticle;
import org.nd.verticles.filtering.SorterVerticle;
import org.nd.verticles.fragment.FragmentExtractorVerticle;
import org.nd.verticles.fs.FilesListsReaderVerticle;
import org.nd.verticles.fs.FileSystemOperationsVerticle;
import org.nd.verticles.operations.Delete;
import org.nd.verticles.operations.PartialUpdate;
import org.nd.verticles.operations.Query;
import org.nd.verticles.operations.SaveOrUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

public class MainApplication {

	private static Logger logger = LoggerFactory.getLogger(ServerVerticle.class);

	public static void main(String[] args) {

		VertxOptions vertxOptions = new VertxOptions();
		vertxOptions.setBlockedThreadCheckInterval(1000 * 60 * 60);
		Vertx vertx = Vertx.vertx(vertxOptions);

		ConfigRetriever retriever = ConfigRetriever.create(vertx);
		retriever.getConfig(json -> {
			JsonObject configObject = json.result();
			
			vertx.deployVerticle(FileSystemOperationsVerticle.class.getName(), new DeploymentOptions().setConfig(configObject), res ->{
				
				vertx.deployVerticle(ServerVerticle.class.getName(), new DeploymentOptions().setConfig(configObject));
				
				vertx.deployVerticle(SaveOrUpdate.class.getName());
				vertx.deployVerticle(Delete.class.getName());
				vertx.deployVerticle(Query.class.getName());
				vertx.deployVerticle(PartialUpdate.class.getName());
				
				vertx.deployVerticle(FilesListsReaderVerticle.class.getName());	
				vertx.deployVerticle(FragmentExtractorVerticle.class.getName());
				
				
				vertx.deployVerticle(SorterVerticle.class.getName());
				vertx.deployVerticle(FilterVerticle.class.getName());
				
				vertx.deployVerticle(ConditionVerifierVerticle.class.getName(), new DeploymentOptions().setWorkerPoolName("json-path-pool")
				        .setWorkerPoolSize(128)
				        .setWorker(true));
				
			});

			

		});

	}

}
