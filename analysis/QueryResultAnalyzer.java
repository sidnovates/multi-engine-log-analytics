package analysis;

import common.sql.DBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class QueryResultAnalyzer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java analysis.QueryResultAnalyzer <execution_id>");
            System.out.println("Example: java analysis.QueryResultAnalyzer 2");
            return;
        }

        int executionId = Integer.parseInt(args[0]);
        analyzeRun(executionId);
    }

    public static void analyzeRun(int executionId) {
        String[] meta = getDatasetName(executionId);

        if (meta == null) {
            System.out.println("Error: Could not find execution_id " + executionId + " in the database.");
            return;
        }

        String datasetName = meta[0];
        String pipelineName = meta[1];

        System.out.println("===============================================================================================================");
        System.out.println("                                      PRIMARY RUN QUERY RESULTS                                                ");
        System.out.println("===============================================================================================================");
        System.out.println("Execution ID: " + executionId);
        System.out.println("Dataset: " + datasetName);
        System.out.println("Pipeline: " + pipelineName.toUpperCase());
        System.out.println("---------------------------------------------------------------------------------------------------------------");
        
        printQ1Primary(executionId);
        printQ2Primary(executionId);
        printQ3Primary(executionId);

        System.out.println("\n===============================================================================================================");
        System.out.println("                                   COMPARATIVE PIPELINE QUERY RESULTS                                            ");
        System.out.println("===============================================================================================================");
        System.out.println("Dataset: " + datasetName);
        System.out.println("---------------------------------------------------------------------------------------------------------------");
        
        printComparativeAnalysis(datasetName, pipelineName);
    }

    private static String[] getDatasetName(int executionId) {
        String sql = "SELECT dataset_name, pipeline_name FROM run_metadata WHERE execution_id = ? LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new String[]{rs.getString("dataset_name"), rs.getString("pipeline_name")};
            }
        } catch (SQLException e) {
            System.err.println("Error fetching dataset name: " + e.getMessage());
        }
        return null;
    }

    // --- PRIMARY RUN DETAILS ---

    private static void printQ1Primary(int executionId) {
        System.out.println("\n[QUERY 1: Daily Traffic]");
        System.out.printf("%-15s | %-12s | %-15s | %-15s%n", "Log Date", "Status Code", "Request Count", "Total Bytes");
        System.out.println("---------------------------------------------------------------------------");

        String sql = "SELECT log_date, status_code, SUM(request_count) AS req_count, SUM(total_bytes) AS tot_bytes " +
                     "FROM daily_traffic " +
                     "WHERE run_id IN (SELECT run_id FROM run_metadata WHERE execution_id = ?) " +
                     "GROUP BY log_date, status_code " +
                     "ORDER BY log_date, status_code";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                String logDate = rs.getString("log_date");
                int statusCode = rs.getInt("status_code");
                long reqCount = rs.getLong("req_count");
                long totBytes = rs.getLong("tot_bytes");
                System.out.printf("%-15s | %-12d | %-15d | %-15d%n", logDate, statusCode, reqCount, totBytes);
            }
            if (!hasData) System.out.println("No data found.");
        } catch (SQLException e) {
            System.err.println("Error in Q1 Primary: " + e.getMessage());
        }
    }

    private static void printQ2Primary(int executionId) {
        System.out.println("\n[QUERY 2: Top Resources]");
        System.out.printf("%-60s | %-15s | %-15s | %-20s%n", "Resource Path", "Request Count", "Total Bytes", "Distinct Host Count");
        System.out.println("------------------------------------------------------------------------------------------------------------------------");

        // Use STRING_AGG for hosts_list.
        String sql = "SELECT resource_path, SUM(request_count) AS req_count, SUM(total_bytes) AS tot_bytes, " +
                     "STRING_AGG(hosts_list, ',') AS combined_hosts " +
                     "FROM top_resources " +
                     "WHERE run_id IN (SELECT run_id FROM run_metadata WHERE execution_id = ?) " +
                     "GROUP BY resource_path " +
                     "ORDER BY req_count DESC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                String resourcePath = rs.getString("resource_path");
                long reqCount = rs.getLong("req_count");
                long totBytes = rs.getLong("tot_bytes");
                String combinedHosts = rs.getString("combined_hosts");

                int distinctHosts = calculateDistinctHosts(combinedHosts);

                System.out.printf("%-60s | %-15d | %-15d | %-20d%n", 
                        truncate(resourcePath, 58), reqCount, totBytes, distinctHosts);
            }
            if (!hasData) System.out.println("No data found.");
        } catch (SQLException e) {
            System.err.println("Error in Q2 Primary: " + e.getMessage());
        }
    }

    private static void printQ3Primary(int executionId) {
        System.out.println("\n[QUERY 3: Hourly Errors]");
        System.out.printf("%-15s | %-10s | %-20s | %-20s | %-12s | %-22s%n", 
                "Log Date", "Log Hour", "Error Request Count", "Total Request Count", "Error Rate", "Distinct Error Hosts");
        System.out.println("-----------------------------------------------------------------------------------------------------------------------------");

        String sql = "SELECT log_date, log_hour, SUM(error_request_count) AS err_req_count, " +
                     "SUM(total_request_count) AS tot_req_count, " +
                     "STRING_AGG(hosts_list, ',') AS combined_hosts " +
                     "FROM hourly_errors " +
                     "WHERE run_id IN (SELECT run_id FROM run_metadata WHERE execution_id = ?) " +
                     "GROUP BY log_date, log_hour " +
                     "ORDER BY log_date, log_hour";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                String logDate = rs.getString("log_date");
                int logHour = rs.getInt("log_hour");
                long errReqCount = rs.getLong("err_req_count");
                long totReqCount = rs.getLong("tot_req_count");
                String combinedHosts = rs.getString("combined_hosts");

                double errorRate = (totReqCount == 0) ? 0.0 : (double) errReqCount / totReqCount;
                int distinctHosts = calculateDistinctHosts(combinedHosts);

                System.out.printf("%-15s | %-10d | %-20d | %-20d | %-12.4f | %-22d%n", 
                        logDate, logHour, errReqCount, totReqCount, errorRate, distinctHosts);
            }
            if (!hasData) System.out.println("No data found.");
        } catch (SQLException e) {
            System.err.println("Error in Q3 Primary: " + e.getMessage());
        }
    }

    // --- COMPARATIVE RUN DETAILS ---

    private static void printComparativeAnalysis(String datasetName, String currentPipeline) {
        // Find the fastest execution_id for each OTHER pipeline on this dataset
        String getFastestSql = "WITH ExecutionStats AS (" +
                               "    SELECT execution_id, pipeline_name, SUM(runtime) AS total_runtime " +
                               "    FROM run_metadata WHERE dataset_name = ? AND pipeline_name != ? " +
                               "    GROUP BY execution_id, pipeline_name " +
                               ") " +
                               "SELECT DISTINCT ON (pipeline_name) pipeline_name, execution_id " +
                               "FROM ExecutionStats ORDER BY pipeline_name, total_runtime ASC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(getFastestSql)) {
            
            pstmt.setString(1, datasetName);
            pstmt.setString(2, currentPipeline);
            ResultSet rs = pstmt.executeQuery();

            boolean hasPipelines = false;
            while (rs.next()) {
                hasPipelines = true;
                String pipeline = rs.getString("pipeline_name");
                int bestExecutionId = rs.getInt("execution_id");
                
                System.out.println("\n\n###############################################################################################################");
                System.out.println("                        PIPELINE: " + pipeline.toUpperCase() + " (Fastest Execution ID: " + bestExecutionId + ")                        ");
                System.out.println("###############################################################################################################");
                
                printQ1Primary(bestExecutionId);
                printQ2Primary(bestExecutionId);
                printQ3Primary(bestExecutionId);
            }

            if (!hasPipelines) {
                System.out.println("No comparative data found for dataset: " + datasetName);
            }
        } catch (SQLException e) {
            System.err.println("Error fetching comparative pipelines: " + e.getMessage());
        }
    }

    // --- UTILITIES ---

    private static int calculateDistinctHosts(String combinedHosts) {
        if (combinedHosts == null || combinedHosts.trim().isEmpty()) {
            return 0;
        }
        String[] hosts = combinedHosts.split(",");
        Set<String> uniqueHosts = new HashSet<>();
        for (String host : hosts) {
            String trimmed = host.trim();
            if (!trimmed.isEmpty() && !trimmed.equals("null")) {
                uniqueHosts.add(trimmed);
            }
        }
        return uniqueHosts.size();
    }

    private static String truncate(String str, int maxLen) {
        if (str == null) return "";
        if (str.length() <= maxLen) return str;
        return str.substring(0, maxLen - 3) + "...";
    }
}
