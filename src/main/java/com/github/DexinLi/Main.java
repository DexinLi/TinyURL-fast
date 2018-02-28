package com.github.DexinLi;

import io.vertx.core.Vertx;

public class Main {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        TinyURLServer server = new TinyURLServer(9000);
        vertx.deployVerticle(server);
    }
}