package analysis;

import common.sql.DBConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RunHistoryProvider {

    public static void main(String[] args) {
        String sql = "SELECT run_id, pipeline_name, dataset_name, execution_timestamp " +
                     "FROM run_metadata " +
                     "WHERE total_runtime > 0 " + // Only show completed runs
                     "ORDER BY run_id DESC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            System.out.println("[");
            boolean first = true;
            while (rs.next()) {
                if (!first) System.out.println(",");
                
                int id = rs.getInt("run_id");
                String pipeline = rs.getString("pipeline_name");
                String dataset = rs.getString("dataset_name");
                Timestamp ts = rs.getTimestamp("execution_timestamp");

                System.out.printf("  {\"id\": %d, \"pipeline\": \"%s\", \"dataset\": \"%s\", \"timestamp\": \"%s\"}",
                        id, pipeline, dataset, ts.toString());
                
                first = false;
            }
            System.out.println("\n]");

        } catch (SQLException e) {
            System.err.println("Error fetching history: " + e.getMessage());
            System.out.println("[]");
        }
    }
}
