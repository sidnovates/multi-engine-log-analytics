package analysis;

import common.sql.DBConnection;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.renderer.category.BarRenderer;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class QueryTimingAnalyzer {

    private static final String GRAPH_DIR = "analysis/graphs/Performance/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java analysis.PerformanceAnalyzer <execution_id>");
            return;
        }

        int executionId = Integer.parseInt(args[0]);

        // Ensure graph directory exists
        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        analyzePerformance(executionId);
    }

    public static void analyzePerformance(int runId) {
        System.out.println("======================================================================");
        System.out.println("                        PERFORMANCE PANEL                             ");
        System.out.println("======================================================================");

        try (Connection conn = DBConnection.getConnection()) {
            // 1. Get Dataset Name and Engine for this execution
            String infoSql = "SELECT dataset_name, pipeline_name FROM run_metadata WHERE run_id = ?";
            String datasetName = "";
            String currentEngine = "";
            try (PreparedStatement pstmt = conn.prepareStatement(infoSql)) {
                pstmt.setInt(1, runId);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    datasetName = rs.getString("dataset_name");
                    currentEngine = rs.getString("pipeline_name");
                }
            }

            System.out.println("Run ID:       " + runId);
            System.out.println("Dataset:      " + datasetName);
            System.out.println("Engine:       " + currentEngine.toUpperCase());
            System.out.println("----------------------------------------------------------------------");

            // 2. Query Timing Comparison (Q1 vs Q2 vs Q3) - Current Execution
            analyzeQueryComparison(conn, runId, currentEngine);

            // 3. Batch-wise Comparison (Current Execution)
            analyzeBatchComparison(conn, runId, currentEngine);

            // 4. Cross-Pipeline Comparison (Same Dataset)
            analyzeCrossPipelineComparison(conn, datasetName, currentEngine, runId);

        } catch (SQLException e) {
            System.err.println("Error during performance analysis: " + e.getMessage());
        }
        System.out.println("======================================================================");
    }

    private static void analyzeQueryComparison(Connection conn, int runId, String engine) throws SQLException {
        System.out.println("\n[PART 1] Total Query Timings (ms):");
        String sql = "SELECT query_number, SUM(query_runtime) as total_time FROM query_metadata " +
                     "WHERE run_id = ? GROUP BY query_number ORDER BY query_number";

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int qNum = rs.getInt("query_number");
                long time = rs.getLong("total_time");
                System.out.printf("Query %d: %d ms\n", qNum, time);
                dataset.addValue(time, "Total Duration", "Q" + qNum);
            }
        }

        saveChart(dataset, "Query Comparison - Run " + runId, "Query", "Time (ms)", "query_comparison_" + runId + ".png");
    }

    private static void analyzeBatchComparison(Connection conn, int runId, String engine) throws SQLException {
        System.out.println("\n[PART 2] Batch-wise Comparison (Current Execution):");
        String sql = "SELECT batch_no, query_number, query_runtime FROM query_metadata " +
                     "WHERE run_id = ? ORDER BY batch_no, query_number";

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int batchNo = rs.getInt("batch_no");
                int qNum = rs.getInt("query_number");
                long time = rs.getLong("query_runtime");
                dataset.addValue(time, "Q" + qNum, "Batch " + batchNo);
            }
        }

        saveLineChart(dataset, "Batch-wise Timing - " + engine.toUpperCase(), "Batch", "Time (ms)", "batch_comparison_" + runId + ".png");
        System.out.println("-> Batch performance graph generated.");
    }

    private static void analyzeCrossPipelineComparison(Connection conn, String datasetName, String currentEngine, int runId) throws SQLException {
        System.out.println("\n[PART 3] Cross-Pipeline Comparison (Dataset: " + datasetName + "):");
        String sql = "SELECT rm.pipeline_name, qm.query_number, AVG(qm.query_runtime) as avg_time " +
                     "FROM query_metadata qm " +
                     "JOIN run_metadata rm ON qm.run_id = rm.run_id " +
                     "WHERE qm.run_id = ? " + // THE TARGET RUN
                     "   OR qm.run_id IN ( " + // THE BEST RECENT SUCCESSFUL RUNS
                     "      SELECT DISTINCT ON (pipeline_name) run_id " +
                     "      FROM run_metadata " +
                     "      WHERE dataset_name = ? AND pipeline_name != ? " +
                     "        AND total_runtime > 0 " +
                     "        AND total_record_count > 0 " +
                     "        AND total_malformed_record_count < total_record_count " +
                     "      ORDER BY pipeline_name, execution_timestamp DESC " +
                     "   ) " +
                     "GROUP BY rm.pipeline_name, qm.query_number ORDER BY rm.pipeline_name, qm.query_number";

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, runId);
            pstmt.setString(2, datasetName);
            pstmt.setString(3, currentEngine);
            ResultSet rs = pstmt.executeQuery();
            boolean dataFound = false;
            while (rs.next()) {
                dataFound = true;
                String engine = rs.getString("pipeline_name");
                int qNum = rs.getInt("query_number");
                double avgTime = rs.getDouble("avg_time");
                System.out.printf("[%s] Q%d Avg: %.2f ms\n", engine.toUpperCase(), qNum, avgTime);
                dataset.addValue(avgTime, engine.toUpperCase(), "Q" + qNum);
            }
            if (!dataFound) System.out.println("No other pipeline data found for this dataset.");
        }

        saveChart(dataset, "Pipeline Comparison - Dataset: " + datasetName, "Query", "Avg Time (ms)", "pipeline_comparison_" + datasetName.replace("/", "_") + ".png");
    }

    private static void saveChart(DefaultCategoryDataset dataset, String title, String xLabel, String yLabel, String fileName) {
        JFreeChart chart = ChartFactory.createBarChart(
                title, xLabel, yLabel, dataset,
                PlotOrientation.VERTICAL, true, true, false);

        chart.setBackgroundPaint(Color.WHITE);
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 18));
        
        // Modern styling
        chart.getCategoryPlot().setBackgroundPaint(new Color(245, 245, 245));
        chart.getCategoryPlot().setRangeGridlinePaint(Color.GRAY);

        // Show values at the top of the bars
        BarRenderer renderer = (BarRenderer) chart.getCategoryPlot().getRenderer();
        renderer.setDefaultItemLabelsVisible(true);
        renderer.setDefaultItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setDefaultItemLabelFont(new Font("SansSerif", Font.BOLD, 12));

        try {
            ChartUtils.saveChartAsPNG(new File(GRAPH_DIR + fileName), chart, 800, 600);
        } catch (IOException e) {
            System.err.println("Error saving chart: " + e.getMessage());
        }
    }

    private static void saveLineChart(DefaultCategoryDataset dataset, String title, String xLabel, String yLabel, String fileName) {
        JFreeChart chart = ChartFactory.createLineChart(
                title, xLabel, yLabel, dataset,
                PlotOrientation.VERTICAL, true, true, false);

        chart.setBackgroundPaint(Color.WHITE);
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 18));
        
        org.jfree.chart.plot.CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(new Color(245, 245, 245));
        plot.setRangeGridlinePaint(Color.GRAY);
        plot.setDomainGridlinePaint(new Color(220, 220, 220));

        org.jfree.chart.renderer.category.LineAndShapeRenderer renderer = new org.jfree.chart.renderer.category.LineAndShapeRenderer();
        renderer.setDefaultShapesVisible(true);
        renderer.setDefaultStroke(new BasicStroke(2.0f));
        plot.setRenderer(renderer);

        try {
            ChartUtils.saveChartAsPNG(new File(GRAPH_DIR + fileName), chart, 800, 600);
        } catch (IOException e) {
            System.err.println("Error saving line chart: " + e.getMessage());
        }
    }
}
