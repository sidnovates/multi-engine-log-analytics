package Query3.MongoDB;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import common.sql.MetadataDAO;
import common.sql.Q3DAO;

import java.io.*;
import java.util.*;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.*;

public class Q3Mongo {

    public static void run(String filePath, int runId) {

        long startTime = System.currentTimeMillis();
        int malformedCount = 0;
        int recordCount = 0;

        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {

            MongoDatabase db = mongoClient.getDatabase("logDB");
            MongoCollection<Document> collection = db.getCollection("logs");

            collection.drop(); // clear previous batch

            // 🔷 STEP 1: Read + Parse + Insert
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            String line;
            List<Document> docs = new ArrayList<>();

            while ((line = br.readLine()) != null) {

                ParsedLog log = LogParser.parse(line);

                if (log == null) {
                    malformedCount++;
                    continue;
                }

                Document doc = new Document()
                        .append("host", log.host)
                        .append("logDate", log.logDate)
                        .append("logHour", log.logHour)
                        .append("method", log.method)
                        .append("resource", log.resource)
                        .append("protocol", log.protocol)
                        .append("status", log.statusCode)
                        .append("bytes", log.bytes);

                docs.add(doc);
                recordCount++;
            }

            if (!docs.isEmpty()) {
                collection.insertMany(docs);
            }

            // 🔷 STEP 2: Aggregation Pipeline
            List<Bson> pipeline = Arrays.asList(

                    // group by date + hour
                    group(
                            new Document("date", "$logDate")
                                    .append("hour", "$logHour"),

                            sum("totalRequests", 1),

                            sum("errorRequests",
                                    new Document("$cond", Arrays.asList(
                                            new Document("$and", Arrays.asList(
                                                    new Document("$gte", Arrays.asList("$status", 400)),
                                                     new Document("$lte", Arrays.asList("$status", 599))
                                            )),
                                            1,
                                            0
                                    ))
                            ),

                            addToSet("errorHosts",
                                    new Document("$cond", Arrays.asList(
                                            new Document("$and", Arrays.asList(
                                                    new Document("$gte", Arrays.asList("$status", 400)),
                                                    new Document("$lte", Arrays.asList("$status", 599))
                                            )),
                                            "$host",
                                            null
                                    ))
                            )
                    ),

                    // project final output
                    new Document("$project",
                            new Document("logDate", "$_id.date")
                                    .append("logHour", "$_id.hour")
                                    .append("totalRequests", 1)
                                    .append("errorRequests", 1)
                                    .append("errorRate",
                                            new Document("$cond", Arrays.asList(
                                                    new Document("$eq", Arrays.asList("$totalRequests", 0)),
                                                    0,
                                                    new Document("$divide",
                                                            Arrays.asList("$errorRequests", "$totalRequests"))
                                            ))
                                    )
                                    .append("distinctErrorHosts",
                                            new Document("$size",
                                                    new Document("$filter",
                                                            new Document("input", "$errorHosts")
                                                                    .append("as", "h")
                                                                    .append("cond",
                                                                            new Document("$ne",
                                                                                    Arrays.asList("$$h", null))
                                                                    )
                                                    )
                                            )
                                    )
                                    .append("filteredErrorHosts",
                                            new Document("$filter",
                                                    new Document("input", "$errorHosts")
                                                            .append("as", "h")
                                                            .append("cond",
                                                                    new Document("$ne",
                                                                            Arrays.asList("$$h", null))
                                                            )
                                            )
                                    )
                                    .append("_id", 0)
                    )
            );

            // 🔷 STEP 3: Execute aggregation
            AggregateIterable<Document> result = collection.aggregate(pipeline);

            // 🔷 STEP 4: Save Results to SQL
            for (Document doc : result) {
                String date = doc.getString("logDate");
                int hour = Integer.parseInt(doc.getString("logHour"));
                long errCount = doc.getInteger("errorRequests").longValue();
                long totalCount = doc.getInteger("totalRequests").longValue();
                double errRate = doc.get("errorRate") instanceof Integer ? 
                                ((Integer)doc.get("errorRate")).doubleValue() : 
                                ((Double)doc.get("errorRate")).doubleValue();
                long distinctErrHosts = doc.getInteger("distinctErrorHosts").longValue();

                List<String> hostsArray = doc.getList("filteredErrorHosts", String.class);
                String hostsList = hostsArray != null ? String.join(",", hostsArray) : "";

                // Save to SQL
                Q3DAO.saveResult(runId, date, hour, errCount, totalCount, errRate, distinctErrHosts, hostsList);
                System.out.println(doc.toJson()); // Print to console for verification
            }

            // 🔷 STEP 5: Final Timing and Runtime Update
            long endTime = System.currentTimeMillis();
            double totalRuntimeSec = (endTime - startTime) / 1000.0;
            MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

            System.out.println("\nTotal Pipeline Runtime: " + (endTime - startTime) + " ms");
            System.out.println("SQL Run ID: " + runId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 🔷 MAIN METHOD
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: java Q3Mongo <input_file_path> <run_id>");
            return;
        }

        run(args[0], Integer.parseInt(args[1]));
    }
}

