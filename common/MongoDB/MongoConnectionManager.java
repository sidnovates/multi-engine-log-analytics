package common.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoConnectionManager {
    private static MongoClient mongoClient = null;

    public static synchronized MongoClient getClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create("mongodb://localhost:27017");
        }
        return mongoClient;
    }

    public static synchronized void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
