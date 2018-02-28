package com.github.DexinLi;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.security.SecureRandom;
import java.util.Random;

public class TinyURLServer extends AbstractVerticle {
    private String mongoHost = "localhost";
    private int mongoPort = 27017;
    private MongoClient mongoClient;
    private String urlCollection = "URL";
    private String numCollection = "num";
    private JsonObject addressField = new JsonObject().put("address", true);
    private JsonObject idField = new JsonObject().put("ID", true);
    private JsonObject numField = new JsonObject().put("num", true);

    private String redisHost = "localhost";
    private int redisPort = 6379;
    private RedisClient redisClient;

    private int serverPort = 80;
    private Random random = new SecureRandom();

    public void setMongoHost(String mongoHost) {
        this.mongoHost = mongoHost;
    }

    public void setMongoPort(int mongoPort) {
        this.mongoPort = mongoPort;
    }

    private String getMongoUri() {
        return "mongodb://" + mongoHost + ':' + mongoPort;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    protected void getIdFromRedis(HttpServerResponse response, String address) {
        redisClient.get(address, stringAsyncResult -> {
            if (stringAsyncResult.succeeded()) {
                String res = stringAsyncResult.result();
                if (res != null) {
                    createRedirection(response, address);
                } else {
                    getIdFromDB(response, address);
                }
            } else {
                //TODO log here
            }
        });
    }

    protected void getIdFromDB(HttpServerResponse response, String address) {
        mongoClient.findOne(urlCollection, new JsonObject().put("address", address), idField, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                JsonObject res = jsonObjectAsyncResult.result();
                String id = res.getString("ID");
                if (id == null) {
                    generateId(response, address);
                } else {
                    putToRedis(address, id);
                    responseSuccess(response, id);
                }
            } else {
                //TODO log here
            }
        });
    }

    protected void generateId(HttpServerResponse response, String address) {
        long num = Math.abs(random.nextLong());
        JsonObject numQuery = new JsonObject().put("num", num);
        mongoClient.findOne(numCollection, numQuery, numField, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                JsonObject res = jsonObjectAsyncResult.result();
                if (res == null) {
                    mongoClient.insert(numCollection, numQuery, stringAsyncResult -> {
                        if (stringAsyncResult.succeeded()) {
                            String id = encodeId(num);
                            putToDB(response, id, address);
                            putToRedis(id, address);
                            putToRedis(address, id);
                        } else {
                            //TODO log here
                        }
                    });
                } else {
                    generateId(response, address);
                }
            } else {
                //TODO log here
            }
        });
    }

    protected String encodeId(long num) {
        StringBuilder sb = new StringBuilder();
        while (num > 0) {
            long t = (num % 64);
            num = num / 64;
            if (t < 10) {
                sb.append((char) (t + '0'));
            } else if (t < 10 + 26) {
                sb.append((char) (t - 10 + 'A'));
            } else if (t == 62) {
                sb.append('-');
            } else if (t == 63) {
                sb.append('_');
            } else {
                sb.append((char) (t - 36 + 'a'));
            }
        }
        return sb.toString();
    }

    protected void putToDB(HttpServerResponse response, String id, String address) {
        JsonObject query = new JsonObject().put("ID", id).put("address", address);
        mongoClient.insert(urlCollection, query, stringAsyncResult -> {
            if (stringAsyncResult.succeeded()) {
                responseSuccess(response, id);
            } else {
                //TODO log here
            }
        });
    }

    protected void getAddressFromRedis(HttpServerResponse response, String id) {
        redisClient.get(id, stringAsyncResult -> {
            if (stringAsyncResult.succeeded()) {
                String res = stringAsyncResult.result();
                if (res != null) {
                    responseSuccess(response, id);
                } else {
                    getAddressFromDB(response, id);
                }
            } else {
                //TODO log
            }
        });
    }

    protected void putToRedis(String key, String value) {
        redisClient.set(key, value, voidAsyncResult -> {
            if (voidAsyncResult.failed()) {
                //TODO log
            }
        });
    }

    protected void getAddressFromDB(HttpServerResponse response, String id) {
        mongoClient.findOne(urlCollection, new JsonObject().put("ID", id), new JsonObject(), jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                JsonObject res = jsonObjectAsyncResult.result();
                String address = res.getString("address");
                if (address != null) {
                    putToRedis(id, address);
                    createRedirection(response, address);
                } else {
                    response404(response);
                }
            } else {
                //TODO log
            }
        });
    }

    private void createRedirection(HttpServerResponse response, String address) {
        response.setStatusCode(301).setStatusMessage("Moved Permanently").putHeader("Location", address).end();
    }

    protected void response404(HttpServerResponse response) {
        response.setStatusCode(404).end("Unknown resource!");
    }

    protected void responseSuccess(HttpServerResponse response, String id) {
        response.setStatusCode(200).end(id);
    }

    protected void responseHomePage(HttpServerResponse response) {
        response.putHeader("content-type", "text/html; charset=UTF-8")
                .setChunked(true)
                .write("<html><body><h1>Hello!</h1></body></html>", "UTF-8")
                .end();
    }

    private Handler<HttpServerRequest> responseHandler = httpServerRequest -> {
        String path = httpServerRequest.path().substring(1);
        HttpServerResponse response = httpServerRequest.response();
        switch (httpServerRequest.method()) {
            case GET:
                if (path.isEmpty()) {
                    responseHomePage(response);
                } else {
                    getAddressFromRedis(response, path);
                }
                return;
            case PUT:
                getIdFromRedis(response, path);
                return;
            default:
                response404(response);
                return;
        }
    };

    private void createIndex() {
        JsonObject urlIndex = new JsonObject().put("ID", "hashed").put("address", "hashed");
        JsonObject numIndex = new JsonObject().put("num", "hashed");
        IndexOptions indexOptions = new IndexOptions().background(true);
        mongoClient.createIndexWithOptions(urlCollection, urlIndex, indexOptions, res -> {
            if (res.failed()) {
                //TODO log here
            }
        });
        mongoClient.createIndexWithOptions(numCollection, numIndex, indexOptions, res -> {
            if (res.failed()) {
                //TODO log here
            }
        });
    }

    protected void initMongoDB() {
        JsonObject mongoConfig = new JsonObject().put("connection_string", getMongoUri()).put("db_name", "TinyURL");
        mongoClient = MongoClient.createShared(vertx, mongoConfig);
        mongoClient.createCollection(urlCollection, res -> {
            if (res.failed()) {
                //TODO log here
            }
        });
        mongoClient.createCollection(numCollection, res -> {
            if (res.failed()) {
                //TODO log here
            }
        });
        createIndex();
    }

    protected void initRedis() {
        redisClient = RedisClient.create(vertx, new RedisOptions().setHost(redisHost).setPort(redisPort));
    }

    public TinyURLServer(int port) {
        this.serverPort = port;
    }

    public TinyURLServer() {
        this(9000);
    }

    @Override
    public void start() throws Exception {
        random.setSeed(System.nanoTime());
        initMongoDB();
        initRedis();
        vertx.createHttpServer().requestHandler(responseHandler).listen(serverPort);
    }
}
