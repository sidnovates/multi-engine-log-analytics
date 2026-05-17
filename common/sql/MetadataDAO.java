package common.sql;

import java.sql.*;

public class MetadataDAO {

    /**
     * Inserts run metadata and returns the generated run_id.
     * Called ONCE per pipeline execution.
     */
    public static int insertRunMetadata(String pipelineName, String datasetName, double avgBatchSize) {
        String sql = "INSERT INTO run_metadata (pipeline_name, dataset_name, avg_batch_size) VALUES (?, ?, ?) RETURNING run_id";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, pipelineName.toLowerCase());
            pstmt.setString(2, datasetName);
            pstmt.setDouble(3, avgBatchSize);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            System.err.println("Error inserting run metadata: " + e.getMessage());
        }
        return -1;
    }

    /**
     * Inserts batch metadata.
     * Called once per batch file processed.
     */
    public static void insertBatchMetadata(int runId, int batchNo, long recordCount) {
        String sql = "INSERT INTO batch_metadata (run_id, batch_no, record_count) VALUES (?, ?, ?)";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, runId);
            pstmt.setInt(2, batchNo);
            pstmt.setLong(3, recordCount);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting batch metadata: " + e.getMessage());
        }
    }

    /**
     * Updates batch runtime and malformed count.
     */
    public static void updateBatchStats(int runId, int batchNo, double batchRuntimeSec, long malformedCount) {
        String sql = "UPDATE batch_metadata SET batch_runtime = batch_runtime + ?, malformed_record_count = GREATEST(malformed_record_count, ?) WHERE run_id = ? AND batch_no = ?";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setDouble(1, batchRuntimeSec);
            pstmt.setLong(2, malformedCount);
            pstmt.setInt(3, runId);
            pstmt.setInt(4, batchNo);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating batch stats: " + e.getMessage());
        }
    }

    /**
     * Updates the final totals for the entire pipeline run.
     */
    public static void updateFinalStats(int runId, double totalRuntimeSec, long totalMalformed, long totalRecords) {
        String sql = "UPDATE run_metadata SET total_runtime = total_runtime + ?, total_malformed_record_count = total_malformed_record_count + ?, total_record_count = total_record_count + ? WHERE run_id = ?";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setDouble(1, totalRuntimeSec);
            pstmt.setLong(2, totalMalformed);
            pstmt.setLong(3, totalRecords);
            pstmt.setInt(4, runId);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating final stats: " + e.getMessage());
        }
    }

    /**
     * Saves individual query metadata.
     */
    public static void saveQueryMetadata(int runId, int batchNo, int queryNum, double queryRuntimeMs) {
        String sql = "INSERT INTO query_metadata (run_id, batch_no, query_number, query_runtime) VALUES (?, ?, ?, ?)";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, runId);
            pstmt.setInt(2, batchNo);
            pstmt.setInt(3, queryNum);
            pstmt.setDouble(4, queryRuntimeMs);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving query metadata: " + e.getMessage());
        }
    }
}
