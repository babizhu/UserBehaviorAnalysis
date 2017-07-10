package com.srxk.car.user.behavioranalysis.http;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

import static com.srxk.car.user.behavioranalysis.database.DatabaseVercitle.CONFIG_DB_QUEUE;

/**
 * Created by liulaoye on 17-7-10.
 * http vercitle
 */
@Slf4j
public class HttpServerVerticle extends AbstractVerticle{
    //            public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";
    private String dbQueue = CONFIG_DB_QUEUE;

    @Override
    public void start( Future<Void> startFuture ) throws Exception{
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router( vertx );
        router.route().handler( BodyHandler.create() );
        router.route( "/addBehavior" ).handler( this::addBehavior );
        int portNumber = config().getInteger( "port", 8080 );
        server
                .requestHandler( router::accept )
                .listen( portNumber, ar -> {
                    if( ar.succeeded() ) {
                        log.info( "HTTP server running on port " + portNumber );
                        startFuture.complete();
                    } else {
                        log.error( "Could not start a HTTP server", ar.cause() );
                        startFuture.fail( ar.cause() );
                    }
                } );
    }

    /**
     * 增加一个用户行为
     * http://localhost:8080/addBehavior?user_id=liulaoye&&behaviors_id=11&&terminal_id=FSSFDDFFKFKFKFKFKFKFKFKFKFKFKFKF&&version_id=1.1.3&&os_version=OSX10.1&&ip=192.168.1.21&&arguments={"CARID":12}
     *
     * @param context
     */
    private void addBehavior( RoutingContext context ){

        final String user_id = context.request().getParam( "user_id" );
        if( user_id == null || user_id.isEmpty() ) {
            responseError( context, " user_id" );
            return;
        }
        final String behaviors_id = context.request().getParam( "behaviors_id" );
        if( behaviors_id == null || behaviors_id.isEmpty() ) {
            responseError( context, " behaviors_id" );
            return;
        }
        final String terminal_id = context.request().getParam( "terminal_id" );
        if( terminal_id == null || terminal_id.isEmpty() ) {
            responseError( context, " terminal_id" );
            return;
        }
        final String os_version = context.request().getParam( "os_version" );
        if( os_version == null || os_version.isEmpty() ) {
            responseError( context, " os_version" );
            return;
        }
        final String version_id = context.request().getParam( "version_id" );
        if( version_id == null || version_id.isEmpty() ) {
            responseError( context, " version_id" );
            return;
        }

        final String ip = context.request().getParam( "ip" );
        if( ip == null || ip.isEmpty() ) {
            responseError( context, " ip" );
            return;
        }
        final String arguments = context.request().getParam( "arguments" );
        if( arguments == null || arguments.isEmpty() ) {
            responseError( context, " arguments" );
            return;
        }
        JsonObject request = new JsonObject();

        request.put( "user_id", user_id )
                .put( "behaviors_id", behaviors_id )
                .put( "terminal_id", terminal_id )
                .put( "version_id", version_id )
                .put( "os_version", os_version )
                .put( "ip", ip )
                .put( "arguments", arguments );


        DeliveryOptions options = new DeliveryOptions().addHeader( "action", "addBehavior" );
        vertx.eventBus().send( dbQueue, request, options, reply -> {
            if( reply.succeeded() ) {
                context.response().setStatusCode( 200 ).end( "ok" );
                System.out.println( "ok!!!!!!!!!!!!" );
            } else {
//                context.fail( reply.cause() );
                context.response().setStatusCode( 500 ).end( reply.cause().getMessage() );
            }
        } );
    }

    private void responseError( RoutingContext context, String errorDesc ){
        context.response().setStatusCode( 500 ).end( errorDesc );

    }
}

