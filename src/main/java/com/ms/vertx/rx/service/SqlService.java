package com.ms.vertx.rx.service;

import io.reactivex.Completable;
import io.reactivex.ObservableEmitter;
import io.reactivex.Single;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.reactivex.ext.sql.SQLConnection;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SqlService {
    JDBCClient sqlClient;
    JsonObject sqlConfig;
    Vertx vertx;

    public SqlService(Vertx vertx, JsonObject config) {
        this.sqlConfig = config;
        this.vertx = vertx;

        sqlClient = getSQL(sqlConfig);
    }

    private JDBCClient getSQL(JsonObject sqlConfig) {
        return JDBCClient.createShared(vertx,sqlConfig, "mydb");
    }

    public void sqlFetch(String sql, JsonArray params, ObservableEmitter<JsonArray> observer) {
        Single<SQLConnection> sqlSingle = sqlClient.rxGetConnection();
        JsonArray arr = new JsonArray();
        sqlSingle.doOnSuccess(sqlConn -> {
            Single<ResultSet> rsSingle = sqlConn.rxQueryWithParams(sql,params);
            rsSingle.doOnSuccess(rs -> {
                List<JsonObject> rows = rs.getRows();
                if (rows.isEmpty())
                    observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE,404,"No records found"));
                else {
                    observer.onNext(new JsonArray(rows));
                    observer.onComplete();
                }
            })
                .doOnError(err -> observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE,500,"failure executing query")))
                .doAfterTerminate(() -> sqlConn.close())
                .subscribe();
        })
            .doOnError(err -> observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE,500,"Unable to connect to database")))
            .subscribe();
    }

    public void sqlInsertUpdate(String sql, JsonArray params, ObservableEmitter<JsonArray> observer) {
        Single<SQLConnection> sqlSingle = sqlClient.rxGetConnection();
        JsonArray arr = new JsonArray();
        sqlSingle.timeout(20000, TimeUnit.MILLISECONDS)
            .doOnSuccess(sqlConn -> {
                Single<UpdateResult> rsSingle = sqlConn.rxUpdateWithParams(sql, params);
                rsSingle.doOnSuccess(ur -> {
                    observer.onNext(ur.getKeys());
                    observer.onComplete();
                }
                )
                .doOnError(err -> observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, "failure executing insert/update")))
                .doAfterTerminate(() -> sqlConn.close())
                .subscribe();
        })
        .doOnError(err -> observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, "Unable to connect to database")))
        .subscribe();
    }

    public void sqlDdlExecute(String sql, ObservableEmitter<Boolean> observer) {
        Single<SQLConnection> sqlSingle = sqlClient.rxGetConnection();
        JsonArray arr = new JsonArray();
        sqlSingle.doOnSuccess(sqlConn -> {
           Completable completable = sqlConn.rxExecute(sql);
           completable.doOnComplete( () -> {observer.onNext(true); observer.onComplete();})
                .doOnError(err -> {
                    observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, "failure executing ddl"));
                })
                .doAfterTerminate(() -> sqlConn.close())
                .subscribe();
        })
        .doOnError(err -> observer.onError(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, 500, "Unable to connect to database")))
        .subscribe();
    }

}
