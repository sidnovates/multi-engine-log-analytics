package common.sql;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Q1DAO {

    private static final SimpleDateFormat logDateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    /**
     * Saves a Query 1 result record to the database.
     */
    public static void saveResult(int runId, String dateStr, int statusCode, long requestCount, long totalBytes) {
        String sql = "INSERT INTO daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, runId);
            
            // Convert "01/Jul/1995" to java.sql.Date
            try {
                java.util.Date parsedDate = logDateFormat.parse(dateStr);
                pstmt.setDate(2, new java.sql.Date(parsedDate.getTime()));
            } catch (Exception e) {
                pstmt.setNull(2, Types.DATE);
            }
            
            pstmt.setInt(3, statusCode);
            pstmt.setLong(4, requestCount);
            pstmt.setLong(5, totalBytes);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving Q1 result: " + e.getMessage());
        }
    }
}
