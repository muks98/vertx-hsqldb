package com.ms.vertx.rx.service;

import io.reactivex.Observable;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RxProductService extends AbstractVerticle {
    JsonObject dbConfig;
    SqlService sqlService;
    @Override
    public void start(Future<Void> startFuture) {

        dbConfig = config();
        sqlService = new SqlService(vertx,dbConfig);
        vertx.eventBus().consumer("fetch.products", msg -> this.getProducts(msg));
        vertx.eventBus().consumer("add.product", msg -> this.addProduct(msg));
        vertx.eventBus().consumer("fetch.product", msg -> this.getProduct(msg));
        startFuture.complete();
    }


    public void getProducts(Message msg) {
        String sql = "select id, name, price from products";
        JsonArray jsonArray = new JsonArray();
        Observable<JsonArray> observe = Observable.create( subscriber -> sqlService.sqlFetch(sql,jsonArray,subscriber));
        observe.subscribe(
            next -> msg.reply(next.toBuffer()),
            error -> { ReplyException re = (ReplyException) error; msg.fail(re.failureCode(), re.getMessage());}
        );
    }

    public void getProduct(Message msg) {
        String sql = "select id, name, price from products where id = ?";
        int id = ((Buffer) msg.body()).getInt(0);
        JsonArray jsonArray = new JsonArray().add(id);
        Observable<JsonArray> observe = Observable.create( subscriber -> sqlService.sqlFetch(sql,jsonArray,subscriber));
        observe.subscribe(
            next -> msg.reply(next.toBuffer()),
            error -> { ReplyException re = (ReplyException) error; msg.fail(re.failureCode(), re.getMessage());}
        );
    }

    public void addProduct(Message msg) {
        String sql = "insert into products(name, price) values ?,?";
        JsonObject product = new JsonObject((Buffer) msg.body());
        JsonArray jsonArray = new JsonArray().add(product.getString("name")).add(product.getFloat("price"));
        Observable<JsonArray> observe = Observable.create( subscriber -> sqlService.sqlInsertUpdate(sql,jsonArray,subscriber));
        observe.subscribe(
            next -> msg.reply(next.toBuffer()),
            error -> {
                if (error instanceof ReplyException) {
                    ReplyException re = (ReplyException) error;
                    msg.fail(re.failureCode(), re.getMessage());
                }
                else
                    msg.fail(500,error.getMessage());
                }
        );
    }
}
