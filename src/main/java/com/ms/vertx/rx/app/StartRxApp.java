package com.ms.vertx.rx.app;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

public class StartRxApp {
    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        StringBuffer deploymentId = new StringBuffer();

        Single<String> st = vertx.rxDeployVerticle(DbRxApp.class.getName(), new DeploymentOptions().setConfig(new JsonObject()
            .put("url","jdbc:hsqldb:mem:test?shutdown=true")
            .put("driver_class","org.hsqldb.jdbcDriver")));
        st.doOnSuccess( id -> System.out.println("Started successfully " + id))
          .doOnError(err -> {System.out.println("Error starting " + err.getMessage()); vertx.rxClose();})
          .subscribe();
     }
}
