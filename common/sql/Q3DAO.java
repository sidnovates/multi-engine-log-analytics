package common.sql;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Q3DAO {

    private static final SimpleDateFormat logDateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    /**
     * Saves a Query 3 result record to the database.
     */
    public static void saveResult(int runId, String dateStr, int hour, long errorCount, 
                                 long totalCount, double errorRate, long distinctErrorHosts, String hostsList) {
        String sql = "INSERT INTO hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts, hosts_list) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, runId);
            
            // Convert date string
            try {
                java.util.Date parsedDate = logDateFormat.parse(dateStr);
                pstmt.setDate(2, new java.sql.Date(parsedDate.getTime()));
            } catch (Exception e) {
                pstmt.setNull(2, Types.DATE);
            }
            
            pstmt.setInt(3, hour);
            pstmt.setLong(4, errorCount);
            pstmt.setLong(5, totalCount);
            pstmt.setDouble(6, errorRate);
            pstmt.setLong(7, distinctErrorHosts);
            pstmt.setString(8, hostsList);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving Q3 result: " + e.getMessage());
        }
    }
}
