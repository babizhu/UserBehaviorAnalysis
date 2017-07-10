package com.srxk.car.user.behavioranalysis.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by liulaoye on 17-7-10.
 * 数据库 vercitle
 */
@Slf4j
public class DatabaseVercitle extends AbstractVerticle{
    private JDBCClient dbClient;
    public static final String CONFIG_DB_QUEUE = "db.queue";

    @Override
    public void start( Future<Void> startFuture ) throws Exception{

        JsonObject config = new JsonObject()
                .put( "provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider" )
                .put( "jdbcUrl", config().getString( "jdbcUrl" ) )
                .put( "username", config().getString( "user" ) )
                .put( "password", config().getString( "password" ) )
//                .put("driverClassName", "org.postgresql.Driver")
                .put( "maximumPoolSize", config().getInteger( "maximumPoolSize" ) );

        dbClient = JDBCClient.createShared( vertx, config );
        dbClient.getConnection( ar -> {
            if( ar.failed() ) {
                log.error( "Could not open a database connection", ar.cause() );
                startFuture.fail( ar.cause() );
            } else {
                SQLConnection connection = ar.result();
                connection.execute( "select 1", create -> {
                    connection.close();
                    if( create.failed() ) {
                        log.error( "Database preparation error", create.cause() );
                        startFuture.fail( create.cause() );
                    } else {
                        log.info( "database init success!" );
                        vertx.eventBus().consumer( CONFIG_DB_QUEUE, this::onMessage );
                        startFuture.complete();
                    }
                } );
            }
        } );
    }

    public enum ErrorCodes{
        NO_ACTION_SPECIFIED,
        BAD_ACTION,
        DB_ERROR
    }

    private void onMessage( Message<JsonObject> message ){
        if( !message.headers().contains( "action" ) ) {
            message.fail( ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified" );
        }
        String action = message.headers().get( "action" );
        switch( action ) {
            case "addBehavior":
                addBehavior( message );
                break;
            default:
                message.fail( ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action );
        }
    }

    private void addBehavior( Message<JsonObject> message ){
        JsonObject request = message.body();
        dbClient.getConnection( car -> {
            if( car.succeeded() ) {
                String insertSql = "INSERT INTO `UserBehaviorAnalysis`.`behaviors` (`user_id`, `behaviors_id`, `terminal_id`, `version_id`, `os_version`, `ip`, `arguments`) VALUES " +
                        "(?, ?, ?, ?, ?, ?, ?)";
                SQLConnection connection = car.result();
                JsonArray data = new JsonArray();

                data.add( request.getString( "user_id" ) )
                        .add( request.getString( "behaviors_id" ) )
                        .add( request.getString( "terminal_id" ) )
                        .add( request.getString( "version_id" ) )
                        .add( request.getString( "os_version" ) )
                        .add( request.getString( "ip" ) )
                        .add( request.getString( "arguments" ) );

                connection.updateWithParams( insertSql, data, res -> {
                    connection.close();
                    if( res.succeeded() ) {
                        message.reply( "ok" );
                    } else {
                        reportQueryError( message, res.cause() );
                    }

                } );
            } else {
                reportQueryError( message, car.cause() );
            }
        } );
    }


    private void reportQueryError( Message<JsonObject> message, Throwable cause ){
        log.error( "Database query error", cause );
        message.fail( ErrorCodes.DB_ERROR.ordinal(), cause.getMessage() );
    }
}
