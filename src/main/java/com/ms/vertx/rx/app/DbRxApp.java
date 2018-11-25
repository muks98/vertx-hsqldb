package com.ms.vertx.rx.app;


import com.ms.vertx.rx.service.RxProductService;
import com.ms.vertx.rx.service.SqlService;
import io.reactivex.*;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class DbRxApp extends AbstractVerticle {
    JsonObject config;
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        this.config = this.config();
        JsonArray jsonArray = new JsonArray();

        SqlService sqlService = new SqlService(vertx,config);
        String sqlStr = "CREATE TABLE IF NOT EXISTS products (id INTEGER IDENTITY, name varchar(100), price DECIMAL(10,2))";
        Observable<Boolean> observeDb = Observable.create ( observer -> sqlService.sqlDdlExecute(sqlStr,observer));
        Observable<Boolean> observerOthers = Observable.create( subscriber -> startOtherServices(subscriber, config));
        Observable.merge(observeDb,observerOthers)
            .subscribe(
                    complete -> startFuture.complete(),
                    error -> startFuture.fail(error.getMessage())
            );

    }
    private void  startOtherServices(ObservableEmitter<Boolean> emit, JsonObject config) {
        vertx.rxDeployVerticle(RxProductService.class.getName(), new DeploymentOptions().setConfig(config)).subscribe();
        DbRxAppRouter appRouter = new DbRxAppRouter(vertx);
        appRouter.start();
        vertx.createHttpServer()
            .requestHandler(req -> appRouter.getRouter().accept(req))
            .rxListen(8080)
            .doOnError(err -> {
                System.out.println("Error listening on port " + err.getMessage());
                emit.onError(err);
            })
            .doOnSuccess(success -> emit.onComplete())
            .subscribe();
    }

}
