package com.ms.vertx.rx.app;

import io.reactivex.Single;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;


public class DbRxAppRouter {
    Vertx vertx;
    Router router;
    public DbRxAppRouter(Vertx vertx) {
        this.vertx = vertx;
    }

    public void start() {
        router = Router.router(vertx);
        router.route( HttpMethod.GET,"/showproducts")
                .handler(routingContext -> handleQueryProducts(routingContext))
                .failureHandler(failureContext -> failureContext.fail(501));
        router.route(HttpMethod.POST,"/addproduct")
            .handler(routingContext -> handleAddProduct(routingContext))
            .failureHandler(failureContext -> failureContext.fail(502));
        router.route(HttpMethod.GET,"/getproductbyid")
            .handler(routingContext -> handleGetProduct(routingContext))
             .failureHandler(failureContext -> failureContext.fail(503));
    }

    public Router getRouter(){
        return router;
    }
    private void handleQueryProducts(RoutingContext routingContext) {
        Single<Message<Object>> msg = vertx.eventBus().rxSend("fetch.products","")
            .retry(1)
            .doOnSuccess(data -> {
                Buffer buff = (Buffer) data.body();
                routingContext.response().setStatusCode(200).end(buff.toString());
             })
            .doOnError(err -> {
                System.out.println("Error sending data to fetch products " + err.getMessage());
                routingContext.response().setStatusCode(500).end(err.getMessage());
            });
        msg.subscribe();
    }
    private void handleAddProduct(RoutingContext routingContext) {
            routingContext.request().bodyHandler(payload -> {
                if (payload.length() > 0) {
                    try {
                        Single<Message<Object>> msg = vertx.eventBus().rxSend("add.product", payload.toJsonObject().toBuffer())
                        .doOnSuccess(recvMsg -> {
                            String httpRes = ((Buffer) recvMsg.body()).toString();
                            routingContext.response().setStatusCode(200).end(httpRes);
                        })
                        .doOnError(err -> {
                            String errMsg = err.getMessage();
                            if (err instanceof ReplyException)
                                errMsg = ((ReplyException) err).failureCode() + errMsg;
                            System.out.println("In failed receive message " + errMsg);
                            routingContext.response().setStatusCode(500).end(errMsg);
                        });
                    msg.subscribe();
                    }catch(Exception e) {
                        routingContext.response().setStatusCode(400).end("Send a valid json in the post");
                    }
                }
            });
    }

    private void handleGetProduct(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        if (id == null || ! id.matches("\\d+"))
            routingContext.response().setStatusCode(400).end("id parameter with numeric value required");
        else {
            int numId = Integer.valueOf(id);
            Single<Message<Object>> msg = vertx.eventBus().rxSend("fetch.product",Buffer.buffer().appendInt(numId))
                .retry(1)
                .doOnSuccess(data -> {
                    Buffer buff = (Buffer) data.body();
                    routingContext.response().setStatusCode(200).end(buff.toString());
                })
                .doOnError(err -> {
                    System.out.println("Error sending data to fetch products " + err.getMessage());
                    routingContext.response().setStatusCode(500).end(err.getMessage());
                });
            msg.subscribe();

            }
    }

}
