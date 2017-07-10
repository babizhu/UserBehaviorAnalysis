package com.srxk.car.user.behavioranalysis;

import com.srxk.car.user.behavioranalysis.database.DatabaseVercitle;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by liulaoye on 17-7-10.
 * MainVerticle
 */
@Slf4j
public class MainVerticle extends AbstractVerticle{


    public void start( Future<Void> startFuture ) throws Exception{

        Future<String> dbVerticleDeployment = Future.future();

        DeploymentOptions dbOptions= new DeploymentOptions().setConfig( config().getJsonObject( "db" ) );
        vertx.deployVerticle( new DatabaseVercitle(),dbOptions, dbVerticleDeployment.completer() );
        dbVerticleDeployment.compose( id -> {
            Future<String> httpVerticleDeployment = Future.future();

            DeploymentOptions options= new DeploymentOptions().setInstances( 2 ).setConfig( config().getJsonObject( "server" ) );
            vertx.deployVerticle(
                    "com.srxk.car.user.behavioranalysis.http.HttpServerVerticle",
//                    new HttpServerVerticle(),
                    options,
                    httpVerticleDeployment.completer() );
            return httpVerticleDeployment;

        } ).setHandler( ar -> {
            if( ar.succeeded() ) {
                startFuture.complete();
            } else {
                startFuture.fail( ar.cause() );
            }
        } );
    }

    public static void main( String[] args ) throws IOException{
        final VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setBlockedThreadCheckInterval( 1000000 );
        Vertx vertx = Vertx.vertx( vertxOptions );

        DeploymentOptions options = new DeploymentOptions();
        options.setInstances( 1 );

        String content = new String( Files.readAllBytes( Paths.get( "resources/application-conf.json" ) ) );
        final JsonObject config = new JsonObject( content );

        log.info( config.toString() );
        options.setConfig( config );

        vertx.deployVerticle( MainVerticle.class.getName(), options, res -> {
            if( res.succeeded() ) {
                log.info( " server started " );
            } else {
                res.cause().printStackTrace();
            }
        } );
    }

}
