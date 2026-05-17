package analysis;

import common.sql.DBConnection;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class QueryResultAnalyzer {

    private static final String GRAPH_DIR = "analysis/graphs/Validation/";

    public static void main(String[] args) {
        if (args.length < 1) return;
        int executionId = Integer.parseInt(args[0]);
        analyzeRun(executionId);
    }

    public static void analyzeRun(int executionId) {
        String[] meta = getDatasetName(executionId);
        if (meta == null) {
            System.err.println("Error: No metadata found for run_id " + executionId);
            return;
        }
        
        String datasetName = meta[0];
        String baselinePipeline = meta[1];
        int baselineRunId = executionId;

        // Ensure graph directory exists
        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) dir.mkdirs();

        // 1. Get Baseline Data
        Set<String> baselineQ1 = getQ1Data(baselineRunId);
        Set<String> baselineQ2 = getQ2Data(baselineRunId);
        Set<String> baselineQ3 = getQ3Data(baselineRunId);

        // 2. Find Latest Historical Runs for other engines
        String[] allEngines = {"mr", "pig", "hive", "mongodb"};
        
        Map<String, String> engineStatus = new LinkedHashMap<>();
        Map<String, Integer> engineRunIds = new LinkedHashMap<>();

        boolean globalMismatch = false;

        for (String engine : allEngines) {
            if (engine.equalsIgnoreCase(baselinePipeline)) {
                engineStatus.put(engine, "BASELINE");
                engineRunIds.put(engine, executionId);
                continue;
            }

            int[] histData = getLatestSuccessfulRun(datasetName, engine);
            if (histData == null) {
                engineStatus.put(engine, "NO_HISTORY");
                continue;
            }
            
            int histRunId = histData[0];
            engineRunIds.put(engine, histRunId);

            Set<String> histQ1 = getQ1Data(histRunId);
            Set<String> histQ2 = getQ2Data(histRunId);
            Set<String> histQ3 = getQ3Data(histRunId);

            boolean match = baselineQ1.equals(histQ1) && 
                            baselineQ2.equals(histQ2) && 
                            baselineQ3.equals(histQ3);

            if (match) {
                engineStatus.put(engine, "MATCH");
            } else {
                engineStatus.put(engine, "MISMATCH");
                globalMismatch = true;
            }
        }

        // 3. Draw Scorecard Image
        drawScorecard(executionId, datasetName, baselinePipeline, engineStatus, engineRunIds, !globalMismatch);
    }

    private static void drawScorecard(int executionId, String datasetName, String baselinePipeline, 
                                      Map<String, String> statuses, Map<String, Integer> runIds, boolean isAllClear) {
        int width = 850;
        int height = 550;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = img.createGraphics();

        // Background
        g2d.setColor(new Color(248, 249, 250));
        g2d.fillRect(0, 0, width, height);

        // Anti-aliasing
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

        // Header
        g2d.setColor(new Color(33, 37, 41));
        g2d.setFont(new Font("SansSerif", Font.BOLD, 32));
        g2d.drawString("Data Integrity Scorecard", 40, 70);

        g2d.setFont(new Font("SansSerif", Font.PLAIN, 18));
        g2d.setColor(new Color(108, 117, 125));
        g2d.drawString("Dataset Target: " + datasetName, 40, 105);

        // Global Status Banner
        int bannerY = 140;
        if (isAllClear) {
            g2d.setColor(new Color(212, 237, 218));
            g2d.fillRoundRect(40, bannerY, width - 80, 70, 15, 15);
            g2d.setColor(new Color(21, 87, 36));
            g2d.setFont(new Font("SansSerif", Font.BOLD, 24));
            g2d.drawString("INTEGRITY: SECURE (All Available History Matches)", 70, bannerY + 45);
        } else {
            g2d.setColor(new Color(248, 215, 218));
            g2d.fillRoundRect(40, bannerY, width - 80, 70, 15, 15);
            g2d.setColor(new Color(114, 28, 36));
            g2d.setFont(new Font("SansSerif", Font.BOLD, 24));
            g2d.drawString("INTEGRITY: DISCREPANCY DETECTED", 70, bannerY + 45);
        }

        // Rows
        int startY = 260;
        int rowHeight = 60;
        
        String[] enginesToPrint = {"mr", "pig", "hive", "mongodb"};
        
        for (int i = 0; i < enginesToPrint.length; i++) {
            String eng = enginesToPrint[i];
            String status = statuses.get(eng);
            
            // Engine Name Box
            g2d.setColor(new Color(233, 236, 239));
            g2d.fillRoundRect(40, startY + (i * rowHeight), 160, 40, 10, 10);
            g2d.setColor(Color.BLACK);
            g2d.setFont(new Font("SansSerif", Font.BOLD, 18));
            g2d.drawString(eng.toUpperCase(), 55, startY + (i * rowHeight) + 27);

            g2d.setFont(new Font("SansSerif", Font.BOLD, 18));
            if (status.equals("BASELINE")) {
                g2d.setColor(new Color(23, 162, 184)); // Cyan
                g2d.drawString("CURRENT RUN (BASELINE)", 230, startY + (i * rowHeight) + 27);
            } else if (status.equals("MATCH")) {
                g2d.setColor(new Color(40, 167, 69)); // Green
                g2d.drawString("100% MATCH (Execution ID: " + runIds.get(eng) + ")", 230, startY + (i * rowHeight) + 27);
            } else if (status.equals("MISMATCH")) {
                g2d.setColor(new Color(220, 53, 69)); // Red
                g2d.drawString("MISMATCH (Execution ID: " + runIds.get(eng) + ")", 230, startY + (i * rowHeight) + 27);
            } else {
                g2d.setColor(new Color(253, 126, 20)); // Orange
                g2d.drawString("NO HISTORY ON THIS DATASET", 230, startY + (i * rowHeight) + 27);
            }
        }

        g2d.dispose();
        
        try {
            ImageIO.write(img, "png", new File(GRAPH_DIR + "validation_chart_" + executionId + ".png"));
            System.out.println("-> Validation Scorecard saved to: " + GRAPH_DIR + "validation_chart_" + executionId + ".png");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] getDatasetName(int executionId) {
        String sql = "SELECT dataset_name, pipeline_name FROM run_metadata WHERE run_id = ? LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new String[]{rs.getString("dataset_name"), rs.getString("pipeline_name")};
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    private static int[] getLatestSuccessfulRun(String datasetName, String engine) {
        String sql = "SELECT run_id FROM run_metadata " +
                     "WHERE dataset_name = ? AND pipeline_name = ? AND total_runtime > 0 " +
                     "ORDER BY execution_timestamp DESC LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, datasetName);
            pstmt.setString(2, engine.toLowerCase());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new int[]{rs.getInt("run_id")};
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    private static Set<String> getQ1Data(int runId) {
        Set<String> set = new HashSet<>();
        String sql = "SELECT log_date, status_code, SUM(request_count) AS req_count, SUM(total_bytes) AS tot_bytes " +
                     "FROM daily_traffic WHERE run_id = ? GROUP BY log_date, status_code";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                set.add(rs.getString("log_date") + "|" + rs.getInt("status_code") + "|" + 
                        rs.getLong("req_count") + "|" + rs.getLong("tot_bytes"));
            }
        } catch (SQLException e) {}
        return set;
    }

    private static Set<String> getQ2Data(int runId) {
        Set<String> set = new HashSet<>();
        String sql = "SELECT resource_path, SUM(request_count) AS req_count, SUM(total_bytes) AS tot_bytes, " +
                     "STRING_AGG(hosts_list, ',') AS combined_hosts " +
                     "FROM top_resources WHERE run_id = ? GROUP BY resource_path";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int hosts = calculateDistinctHosts(rs.getString("combined_hosts"));
                set.add(rs.getString("resource_path") + "|" + rs.getLong("req_count") + "|" + 
                        rs.getLong("tot_bytes") + "|" + hosts);
            }
        } catch (SQLException e) {}
        return set;
    }

    private static Set<String> getQ3Data(int runId) {
        Set<String> set = new HashSet<>();
        String sql = "SELECT log_date, log_hour, SUM(error_request_count) AS err_req_count, " +
                     "SUM(total_request_count) AS tot_req_count, " +
                     "STRING_AGG(hosts_list, ',') AS combined_hosts " +
                     "FROM hourly_errors WHERE run_id = ? GROUP BY log_date, log_hour";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int hosts = calculateDistinctHosts(rs.getString("combined_hosts"));
                long err = rs.getLong("err_req_count");
                long tot = rs.getLong("tot_req_count");
                set.add(rs.getString("log_date") + "|" + rs.getInt("log_hour") + "|" + 
                        err + "|" + tot + "|" + hosts);
            }
        } catch (SQLException e) {}
        return set;
    }

    private static int calculateDistinctHosts(String combinedHosts) {
        if (combinedHosts == null || combinedHosts.trim().isEmpty()) return 0;
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
}
