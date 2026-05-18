package Query2.MongoDB;

import com.mongodb.client.*;
import org.bson.Document;
import common.sql.Q2DAO;
import java.util.Arrays;
import java.util.List;

public class Q2Mongo_V2 {
    private static final String REGEX_PATTERN = "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+(HTTP/[\\d.]+)\"\\s+(\\d{3})\\s+(\\d+|-)$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Query2.MongoDB.Q2Mongo_V2 <ignored_input> <run_id>");
            return;
        }
        run(Integer.parseInt(args[1]));
    }

    public static void run(int runId) {
        long startTime = System.currentTimeMillis();
        try {
            MongoClient mongoClient = common.MongoDB.MongoConnectionManager.getClient();
            MongoDatabase db = mongoClient.getDatabase("logDB");
            MongoCollection<Document> collection = db.getCollection("raw_logs");

            List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("raw_line", new Document("$regex", REGEX_PATTERN))),
                new Document("$addFields", new Document("parsed", 
                    new Document("$regexFind", new Document("input", "$raw_line").append("regex", REGEX_PATTERN))
                )),
                new Document("$project", new Document("host", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 0)))
                    .append("resource", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 4)))
                    .append("bytesStr", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 7)))
                ),
                new Document("$project", new Document("host", 1)
                    .append("resource", 1)
                    .append("bytes", new Document("$cond", Arrays.asList(
                        new Document("$eq", Arrays.asList("$bytesStr", "-")),
                        0L,
                        new Document("$toLong", "$bytesStr")
                    )))
                ),
                new Document("$group", new Document("_id", "$resource")
                    .append("requestCount", new Document("$sum", 1))
                    .append("totalBytes", new Document("$sum", "$bytes"))
                    .append("uniqueHostsSet", new Document("$addToSet", "$host"))
                ),
                new Document("$project", new Document("resource", "$_id")
                    .append("requestCount", 1)
                    .append("totalBytes", 1)
                    .append("uniqueHosts", new Document("$size", "$uniqueHostsSet"))
                    .append("uniqueHostsList", new Document("$reduce", new Document("input", "$uniqueHostsSet")
                        .append("initialValue", "")
                        .append("in", new Document("$cond", Arrays.asList(
                            new Document("$eq", Arrays.asList("$$value", "")),
                            "$$this",
                            new Document("$concat", Arrays.asList("$$value", ",", "$$this"))
                        )))
                    ))
                ),
                new Document("$sort", new Document("requestCount", -1).append("_id", -1)),
                new Document("$limit", 20)
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            for (Document doc : result) {
                String resource = doc.getString("resource");
                long count = doc.getInteger("requestCount").longValue();
                long bytes = doc.getLong("totalBytes");
                long hosts = doc.getInteger("uniqueHosts").longValue();
                String hostsList = doc.getString("uniqueHostsList");

                Q2DAO.saveResult(runId, resource, count, bytes, hosts, hostsList);
            }

            long endTime = System.currentTimeMillis();
            System.out.println("\nQuery 2 Runtime (Mongo V2 + SQL): " + (endTime - startTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
