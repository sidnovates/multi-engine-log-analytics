package Query3.MongoDB;

import com.mongodb.client.*;
import org.bson.Document;
import common.sql.Q3DAO;
import java.util.Arrays;
import java.util.List;

public class Q3Mongo_V2 {
    private static final String REGEX_PATTERN = "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+(HTTP/[\\d.]+)\"\\s+(\\d{3})\\s+(\\d+|-)$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Query3.MongoDB.Q3Mongo_V2 <ignored_input> <run_id>");
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
                    .append("logDate", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 1)))
                    .append("logHour", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 2)))
                    .append("status", new Document("$toInt", new Document("$arrayElemAt", Arrays.asList("$parsed.captures", 6))))
                ),
                new Document("$addFields", new Document("isError", new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                                new Document("$gte", Arrays.asList("$status", 400)),
                                new Document("$lte", Arrays.asList("$status", 599))
                        )),
                        1,
                        0
                )))),
                new Document("$group", new Document("_id", new Document("date", "$logDate").append("hour", "$logHour"))
                    .append("totalRequests", new Document("$sum", 1))
                    .append("errorRequests", new Document("$sum", "$isError"))
                    .append("rawErrorHostsSet", new Document("$addToSet", new Document("$cond", Arrays.asList(
                            new Document("$eq", Arrays.asList("$isError", 1)),
                            "$host",
                            null // We push null for non-errors, then filter it out below
                    ))))
                ),
                new Document("$project", new Document("logDate", "$_id.date")
                    .append("logHour", "$_id.hour")
                    .append("totalRequests", 1)
                    .append("errorRequests", 1)
                    .append("errorHostsSet", new Document("$filter", new Document("input", "$rawErrorHostsSet")
                            .append("as", "h")
                            .append("cond", new Document("$ne", Arrays.asList("$$h", null)))
                    ))
                ),
                new Document("$project", new Document("logDate", 1)
                    .append("logHour", 1)
                    .append("totalRequests", 1)
                    .append("errorRequests", 1)
                    .append("errorHosts", new Document("$size", "$errorHostsSet"))
                    .append("errorHostsList", new Document("$reduce", new Document("input", "$errorHostsSet")
                        .append("initialValue", "")
                        .append("in", new Document("$cond", Arrays.asList(
                            new Document("$eq", Arrays.asList("$$value", "")),
                            "$$this",
                            new Document("$concat", Arrays.asList("$$value", ",", "$$this"))
                        )))
                    ))
                )
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            for (Document doc : result) {
                String date = doc.getString("logDate");
                int hour = Integer.parseInt(doc.getString("logHour"));
                long totalCount = doc.getInteger("totalRequests").longValue();
                long errorCount = doc.getInteger("errorRequests").longValue();
                double errorRate = (totalCount == 0) ? 0 : (double) errorCount / totalCount;
                long hosts = doc.getInteger("errorHosts").longValue();
                String hostsList = doc.getString("errorHostsList");

                Q3DAO.saveResult(runId, date, hour, errorCount, totalCount, errorRate, hosts, hostsList);
            }

            long endTime = System.currentTimeMillis();
            System.out.println("\nQuery 3 Runtime (Mongo V2 + SQL): " + (endTime - startTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
