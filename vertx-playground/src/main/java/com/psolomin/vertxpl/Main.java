package com.psolomin.vertxpl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main {
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
  }
}
