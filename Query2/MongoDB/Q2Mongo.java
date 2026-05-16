package Query2.MongoDB;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import common.sql.MetadataDAO;
import common.sql.Q2DAO;

import java.io.*;
import java.util.*;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.*;

public class Q2Mongo {

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

                    // group by resource
                    group("$resource",
                            sum("requestCount", 1),
                            sum("totalBytes", "$bytes"),
                            addToSet("hosts", "$host")
                    ),

                    // project required fields
                    new Document("$project",
                            new Document("resource", "$_id")
                                    .append("requestCount", 1)
                                    .append("totalBytes", 1)
                                    .append("hosts", 1)
                                    .append("distinctHosts",
                                            new Document("$size", "$hosts"))
                                    .append("_id", 0)
                    ),

                    // sort by request count descending
                    sort(descending("requestCount")),

                    // limit to top 20
                    limit(20)
            );

            // 🔷 STEP 3: Execute aggregation
            AggregateIterable<Document> result = collection.aggregate(pipeline);

            // 🔷 STEP 4: Save Results to SQL
            for (Document doc : result) {
                String resource = doc.getString("resource");
                long reqCount = doc.getInteger("requestCount").longValue();
                long totalBytes = doc.get("totalBytes") instanceof Integer ? 
                                 ((Integer)doc.get("totalBytes")).longValue() : 
                                 ((Long)doc.get("totalBytes")).longValue();
                long distinctHosts = doc.getInteger("distinctHosts").longValue();
                
                List<String> hostsArray = doc.getList("hosts", String.class);
                String hostsList = String.join(",", hostsArray);

                // Save to SQL
                Q2DAO.saveResult(runId, resource, reqCount, totalBytes, distinctHosts, hostsList);
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
            System.out.println("Usage: java Q2Mongo <input_file_path> <run_id>");
            return;
        }

        run(args[0], Integer.parseInt(args[1]));
    }
}