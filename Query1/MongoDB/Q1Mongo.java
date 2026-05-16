package Query1.MongoDB;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson; 

import common.sql.MetadataDAO;
import common.sql.Q1DAO;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
public class Q1Mongo {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Q1Mongo <input_file_path> <run_id>");
            return;
        }

        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String filePath, int runId) {

        long startTime = System.currentTimeMillis();
        int malformedCount = 0;
        int recordCount = 0;

        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {

            MongoDatabase db = mongoClient.getDatabase("logDB");
            MongoCollection<Document> collection = db.getCollection("logs");

            collection.drop(); // clear old data

            // 🔷 STEP 1: Read + Parse + Insert
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            String line;
            List<Document> batchDocs = new ArrayList<>();

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

                batchDocs.add(doc);
                recordCount++;
            }

            if (!batchDocs.isEmpty()) {
                collection.insertMany(batchDocs);
            }

            // 🔷 STEP 2: Aggregation
            List<Bson> pipeline = Arrays.asList(
                group(
                    new Document("date", "$logDate")
                            .append("status", "$status"),
                    sum("requestCount", 1),
                    sum("totalBytes", "$bytes")
                )
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            // 🔷 STEP 3: Save Results to SQL
            for (Document doc : result) {
                Document idDoc = (Document) doc.get("_id");
                String date = idDoc.getString("date");
                int status = idDoc.getInteger("status");
                long reqCount = doc.getInteger("requestCount").longValue();
                long totalBytes = doc.get("totalBytes") instanceof Integer ? 
                                 ((Integer)doc.get("totalBytes")).longValue() : 
                                 ((Long)doc.get("totalBytes")).longValue();

                Q1DAO.saveResult(runId, date, status, reqCount, totalBytes);
                System.out.println(doc.toJson());
            }

            // 🔷 STEP 4: Final Timing and Runtime Update
            long endTime = System.currentTimeMillis();
            double totalRuntimeSec = (endTime - startTime) / 1000.0;
            MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

            System.out.println("\nTotal Pipeline Runtime: " + (endTime - startTime) + " ms");
            System.out.println("SQL Run ID: " + runId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}