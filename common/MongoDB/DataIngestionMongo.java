package common.MongoDB;

import com.mongodb.client.*;
import org.bson.Document;
import common.sql.MetadataDAO;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class DataIngestionMongo {
    // The exact Java regex we will use in MongoDB aggregation to verify malformed counts
    private static final String REGEX_PATTERN = "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+(HTTP/[\\d.]+)\"\\s+(\\d{3})\\s+(\\d+|-)$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java common.MongoDB.DataIngestionMongo <input_file_path> <run_id>");
            return;
        }
        run(args[0], Integer.parseInt(args[1]));
    }

    public static void run(String filePath, int runId) {
        try {
            MongoClient mongoClient = MongoConnectionManager.getClient();
            MongoDatabase db = mongoClient.getDatabase("logDB");
            MongoCollection<Document> collection = db.getCollection("raw_logs");

            collection.drop(); // Clear out the collection for the fresh batch

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            List<Document> batchDocs = new ArrayList<>();

            long totalRecords = 0;
            long malformedCount = 0;

            // FAST DUMP: Parse through Java to get exact malformed count, but push raw string to Mongo
            while ((line = br.readLine()) != null) {
                totalRecords++;
                if (common.Parsing.LogParser.parse(line) == null) {
                    malformedCount++;
                }
                
                batchDocs.add(new Document("raw_line", line));
                if (batchDocs.size() >= 50000) {
                    collection.insertMany(batchDocs);
                    batchDocs.clear();
                }
            }
            if (!batchDocs.isEmpty()) {
                collection.insertMany(batchDocs);
            }
            br.close();

            // Instantly update the SQL database
            MetadataDAO.updateFinalStats(runId, 0.0, malformedCount, 0);

            System.out.println("MongoDB Ingestion Complete. Total: " + totalRecords + ", Malformed: " + malformedCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
