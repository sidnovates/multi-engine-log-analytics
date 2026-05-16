package Query1.MongoDB;

import com.mongodb.client.*;
import org.bson.Document;
import common.sql.Q1DAO;
import java.util.Arrays;
import java.util.List;

public class Q1Mongo_V2 {
    private static final String REGEX_PATTERN = "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+(HTTP/[\\d.]+)\"\\s+(\\d{3})\\s+(\\d+|-)$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Query1.MongoDB.Q1Mongo_V2 <ignored_input> <run_id>");
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
                // Stage 1: Drop malformed lines instantly using the regex
                new Document("$match", new Document("raw_line", new Document("$regex", REGEX_PATTERN))),
                
                // Stage 2: Extract the regex groups into an array
                new Document("$addFields", new Document("parsed", 
                    new Document("$regexFind", new Document("input", "$raw_line").append("regex", REGEX_PATTERN))
                )),
                
                // Stage 3: Grab the exact capture groups we need for Query 1
                new Document("$project", new Document("logDate", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 1)))
                    .append("status", new Document("$toInt", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 6))))
                    .append("bytesStr", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 7)))
                ),
                
                // Stage 4: Dash-to-Zero mapping and type casting
                new Document("$project", new Document("logDate", 1)
                    .append("status", 1)
                    .append("bytes", new Document("$cond", Arrays.asList(
                        new Document("$eq", Arrays.asList("$bytesStr", "-")),
                        0L,
                        new Document("$toLong", "$bytesStr")
                    )))
                ),
                
                // Stage 5: Final Grouping logic for Query 1
                new Document("$group", new Document("_id", new Document("date", "$logDate").append("status", "$status"))
                    .append("requestCount", new Document("$sum", 1))
                    .append("totalBytes", new Document("$sum", "$bytes"))
                )
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            for (Document doc : result) {
                Document idDoc = (Document) doc.get("_id");
                String date = idDoc.getString("date");
                int status = idDoc.getInteger("status");
                long reqCount = doc.getInteger("requestCount").longValue();
                long totalBytes = doc.getLong("totalBytes");

                Q1DAO.saveResult(runId, date, status, reqCount, totalBytes);
            }

            long endTime = System.currentTimeMillis();
            System.out.println("\nQuery 1 Runtime (Mongo V2 + SQL): " + (endTime - startTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
