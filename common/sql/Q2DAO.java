package common.sql;

import java.sql.*;

public class Q2DAO {

    /**
     * Saves a Query 2 result record to the database.
     */
    public static void saveResult(int runId, String resourcePath, long requestCount, 
                                 long totalBytes, long distinctHostCount, String hostsList) {
        String sql = "INSERT INTO top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count, hosts_list) " +
                     "VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, runId);
            pstmt.setString(2, resourcePath);
            pstmt.setLong(3, requestCount);
            pstmt.setLong(4, totalBytes);
            pstmt.setLong(5, distinctHostCount);
            pstmt.setString(6, hostsList);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving Q2 result: " + e.getMessage());
        }
    }
}
