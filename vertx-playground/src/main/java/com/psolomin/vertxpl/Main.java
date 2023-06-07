package com.psolomin.vertxpl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main {
  static class Server extends AbstractVerticle {
    public void start() {
      vertx
          .createHttpServer()
          .requestHandler(
              req -> {
                req.response().putHeader("content-type", "text/plain").end("Hello from Vert.x!");
              })
          .listen(8080);
    }
  }

  static class MyVerticle extends AbstractVerticle {
    @Override
    public void start() {
      JsonObject config = context.config();
      Thread t = Thread.currentThread();
      System.out.println("Started " + t.getName() + ". Config" + config);
    }

    @Override
    public void stop() {
      Thread t = Thread.currentThread();
      System.out.println("Stop" + t.getName());
    }
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    MyVerticle v = new MyVerticle();
    v.init(vertx, vertx.getOrCreateContext());
    v.start();
    v.stop();

    Server s = new Server();
    s.init(vertx, vertx.getOrCreateContext());
    s.start();
  }
}
