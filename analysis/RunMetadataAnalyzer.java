package analysis;

import common.sql.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RunMetadataAnalyzer {

    /**
     * Entry point for testing the analyzer.
     * Provide an execution_id as the first argument.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java analysis.RunMetadataAnalyzer <execution_id>");
            System.out.println("Example: java analysis.RunMetadataAnalyzer 1");
            return;
        }

        int executionId = Integer.parseInt(args[0]);
        analyzeRun(executionId);
    }

    /**
     * Fetches and prints the metadata for a specific execution, and then displays
     * the comparative analysis against other pipelines using the same dataset.
     */
    public static void analyzeRun(int executionId) {
        String[] meta = printPrimaryRunDetails(executionId);

        if (meta != null) {
            String datasetName = meta[0];
            String pipelineName = meta[1];
            printComparativeAnalysis(datasetName, pipelineName);
        } else {
            System.out.println("Error: Could not find execution_id " + executionId + " in the database.");
        }
    }

    /**
     * Queries PostgreSQL for the target execution's aggregated metadata and prints it neatly.
     * Returns a string array [dataset_name, pipeline_name] so it can be used for the comparative query.
     */
    private static String[] printPrimaryRunDetails(int executionId) {
        String sql = "SELECT pipeline_name, dataset_name, " +
                     "COUNT(batch_no) AS total_batches, " +
                     "AVG(avg_batch_size) AS average_batch_size, " +
                     "SUM(total_runtime) AS total_runtime, " +
                     "SUM(total_malformed_record_count) AS total_malformed " +
                     "FROM run_metadata " +
                     "WHERE run_id = ? " +
                     "GROUP BY pipeline_name, dataset_name";

        String datasetName = null;
        String pipelineName = null;

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                pipelineName = rs.getString("pipeline_name");
                datasetName = rs.getString("dataset_name");
                int totalBatches = rs.getInt("total_batches");
                double avgBatchSize = rs.getDouble("average_batch_size");
                double totalRuntime = rs.getDouble("total_runtime");
                int totalMalformed = rs.getInt("total_malformed");

                System.out.println("==================================================");
                System.out.println("              TARGET RUN ANALYSIS                 ");
                System.out.println("==================================================");
                System.out.printf("Execution ID:       %d%n", executionId);
                System.out.printf("Pipeline:           %s%n", pipelineName.toUpperCase());
                System.out.printf("Dataset:            %s%n", datasetName);
                System.out.printf("Total Batches:      %d%n", totalBatches);
                System.out.printf("Average Batch Size: %.2f%n", avgBatchSize);
                System.out.printf("Total Runtime:      %.3f sec%n", totalRuntime);
                System.out.printf("Total Malformed:    %d%n", totalMalformed);
                System.out.println("==================================================\n");
            }
        } catch (SQLException e) {
            System.err.println("Database error while fetching primary run details: " + e.getMessage());
        }

        if (datasetName != null && pipelineName != null) {
            return new String[]{datasetName, pipelineName};
        }
        return null;
    }

    /**
     * Aggregates batches by execution_id and finds the fastest full execution 
     * (minimum total runtime) for each OTHER pipeline that processed the given dataset.
     */
    private static void printComparativeAnalysis(String datasetName, String currentPipeline) {
        String sql = "WITH LatestSuccessfulRuns AS (" +
                     "    SELECT DISTINCT ON (pipeline_name) " +
                     "           pipeline_name, run_id, " +
                     "           total_batches, " +
                     "           average_batch_size, " +
                     "           total_runtime, " +
                     "           total_malformed_record_count " +
                     "    FROM run_metadata " +
                     "    WHERE dataset_name = ? AND pipeline_name != ? " +
                     "      AND total_runtime > 0 " +
                     "      AND total_record_count > 1000 " +
                     "    ORDER BY pipeline_name, execution_timestamp DESC " +
                     ") " +
                     "SELECT * FROM LatestSuccessfulRuns ORDER BY total_runtime ASC";

        System.out.println("=================================================================================================");
        System.out.println("                               COMPARATIVE PIPELINE PERFORMANCE                                  ");
        System.out.println("                     (Fastest full execution per OTHER pipeline on same dataset)                 ");
        System.out.println("=================================================================================================");
        System.out.printf("%-15s | %-12s | %-13s | %-15s | %-15s | %-10s%n", 
                          "Pipeline", "Execution ID", "Total Batches", "Avg Batch Size", "Total Runtime", "Malformed");
        System.out.println("-------------------------------------------------------------------------------------------------");

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, datasetName);
            pstmt.setString(2, currentPipeline);
            ResultSet rs = pstmt.executeQuery();

            boolean found = false;
            while (rs.next()) {
                found = true;
                String pipeline = rs.getString("pipeline_name").toUpperCase();
                int runId = rs.getInt("run_id");
                int totalBatches = rs.getInt("total_batches");
                double avgBatchSize = rs.getDouble("average_batch_size");
                double totalRuntime = rs.getDouble("total_runtime");
                int totalMalformed = rs.getInt("total_malformed_record_count");

                System.out.printf("%-15s | %-12d | %-13d | %-15.2f | %-15.3f | %-10d%n", 
                                  pipeline, runId, totalBatches, avgBatchSize, totalRuntime, totalMalformed);
            }

            if (!found) {
                System.out.println("No comparative data found for dataset: " + datasetName);
            }

        } catch (SQLException e) {
            System.err.println("Database error while fetching comparative analysis: " + e.getMessage());
        }
        System.out.println("=================================================================================================\n");
    }
}
