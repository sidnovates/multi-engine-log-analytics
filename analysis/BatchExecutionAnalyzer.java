package analysis;

import common.sql.DBConnection;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.Color;
import java.awt.BasicStroke;
import java.awt.Font;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.ui.RectangleInsets;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BatchExecutionAnalyzer {

    private static final String GRAPH_DIR = "analysis/graphs/BatchExecution/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java analysis.BatchExecutionAnalyzer <execution_id>");
            System.out.println("Example: java analysis.BatchExecutionAnalyzer 2");
            return;
        }

        int executionId = Integer.parseInt(args[0]);

        // Ensure graph directory exists
        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        analyzeRun(executionId);
    }

    public static void analyzeRun(int executionId) {
        String[] meta = getExecutionMetadata(executionId);
        if (meta == null) {
            System.out.println("Error: Could not find execution_id " + executionId + " in the database.");
            return;
        }

        String pipelineName = meta[0];
        String datasetName = meta[1];

        System.out.println("======================================================================");
        System.out.println("                      BATCH-WISE EXECUTION VIEW                       ");
        System.out.println("======================================================================");
        System.out.println("Execution ID: " + executionId);
        System.out.println("Pipeline:     " + pipelineName.toUpperCase());
        System.out.println("Dataset:      " + datasetName);
        System.out.println("----------------------------------------------------------------------");

        printPrimaryBatchDetails(executionId);
        generatePrimaryGraph(executionId, pipelineName);

        System.out.println("\nGenerating Comparative Execution Graph...");
        generateComparativeGraph(executionId, pipelineName, datasetName);
    }

    private static String[] getExecutionMetadata(int executionId) {
        String sql = "SELECT pipeline_name, dataset_name FROM run_metadata WHERE execution_id = ? LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new String[]{rs.getString("pipeline_name"), rs.getString("dataset_name")};
            }
        } catch (SQLException e) {
            System.err.println("Error fetching metadata: " + e.getMessage());
        }
        return null;
    }

    private static void printPrimaryBatchDetails(int executionId) {
        System.out.printf("%-10s | %-20s | %-15s | %-15s%n", "Batch ID", "Records (batch_size)", "Runtime (sec)", "Malformed Count");
        System.out.println("----------------------------------------------------------------------");

        String sql = "SELECT batch_id, batch_size, runtime, malformed_record_count " +
                     "FROM run_metadata WHERE execution_id = ? ORDER BY batch_id ASC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                int batchId = rs.getInt("batch_id");
                int records = rs.getInt("batch_size");
                double runtime = rs.getDouble("runtime");
                int malformed = rs.getInt("malformed_record_count");

                System.out.printf("%-10d | %-20d | %-15.3f | %-15d%n", batchId, records, runtime, malformed);
            }
            if (!hasData) System.out.println("No batch data found.");
        } catch (SQLException e) {
            System.err.println("Error fetching batch details: " + e.getMessage());
        }
    }

    private static void generatePrimaryGraph(int executionId, String pipelineName) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        String seriesName = pipelineName.toUpperCase() + " (Exec ID: " + executionId + ")";

        String sql = "SELECT batch_id, runtime FROM run_metadata WHERE execution_id = ? ORDER BY batch_id ASC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                int batchId = rs.getInt("batch_id");
                double runtime = rs.getDouble("runtime");
                dataset.addValue(runtime, seriesName, "B" + batchId);
            }
        } catch (SQLException e) {
            System.err.println("Error fetching data for primary graph: " + e.getMessage());
            return;
        }

        JFreeChart lineChartObject = ChartFactory.createLineChart(
                "Batch Runtime Analysis - Execution " + executionId,
                "Batch ID",
                "Runtime (seconds)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        applyModernTheme(lineChartObject);

        int width = 800;    /* Width of the image */
        int height = 600;   /* Height of the image */
        File lineChart = new File(GRAPH_DIR + "primary_run_" + executionId + ".png");

        try {
            ChartUtils.saveChartAsPNG(lineChart, lineChartObject, width, height);
            System.out.println("-> Primary graph saved to: " + lineChart.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving primary graph: " + e.getMessage());
        }
    }

    private static void generateComparativeGraph(int currentExecutionId, String currentPipeline, String datasetName) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // 1. Add current execution to the graph
        String seriesNameCurrent = currentPipeline.toUpperCase() + " (Target Exec: " + currentExecutionId + ")";
        addExecutionDataToChart(dataset, currentExecutionId, seriesNameCurrent);

        // 2. Find fastest execution_id for OTHER pipelines on same dataset
        String getFastestSql = "WITH ExecutionStats AS (" +
                               "    SELECT execution_id, pipeline_name, SUM(runtime) AS total_runtime " +
                               "    FROM run_metadata " +
                               "    WHERE dataset_name = ? AND pipeline_name != ? " +
                               "    GROUP BY execution_id, pipeline_name " +
                               ") " +
                               "SELECT DISTINCT ON (pipeline_name) pipeline_name, execution_id " +
                               "FROM ExecutionStats ORDER BY pipeline_name, total_runtime ASC";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(getFastestSql)) {
            
            pstmt.setString(1, datasetName);
            pstmt.setString(2, currentPipeline);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String otherPipeline = rs.getString("pipeline_name");
                int otherExecId = rs.getInt("execution_id");
                
                String seriesNameOther = otherPipeline.toUpperCase() + " (Fastest Exec: " + otherExecId + ")";
                addExecutionDataToChart(dataset, otherExecId, seriesNameOther);
            }
        } catch (SQLException e) {
            System.err.println("Error fetching comparative execution data: " + e.getMessage());
            return;
        }

        JFreeChart lineChartObject = ChartFactory.createLineChart(
                "Comparative Batch Runtime - Dataset: " + datasetName,
                "Batch ID",
                "Runtime (seconds)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        applyModernTheme(lineChartObject);

        int width = 1000;   /* Width of the image */
        int height = 600;   /* Height of the image */
        File lineChart = new File(GRAPH_DIR + "comparative_run_" + currentExecutionId + ".png");

        try {
            ChartUtils.saveChartAsPNG(lineChart, lineChartObject, width, height);
            System.out.println("-> Comparative graph saved to: " + lineChart.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving comparative graph: " + e.getMessage());
        }
    }

    private static void addExecutionDataToChart(DefaultCategoryDataset dataset, int executionId, String seriesName) {
        String sql = "SELECT batch_id, runtime FROM run_metadata WHERE execution_id = ? ORDER BY batch_id ASC";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int batchId = rs.getInt("batch_id");
                double runtime = rs.getDouble("runtime");
                dataset.addValue(runtime, seriesName, "B" + batchId);
            }
        } catch (SQLException e) {
            System.err.println("Error adding data to chart: " + e.getMessage());
        }
    }
    private static void applyModernTheme(JFreeChart chart) {
        chart.setBackgroundPaint(Color.WHITE);
        
        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(new Color(245, 245, 245));
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setOutlineVisible(false);

        LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();
        renderer.setDefaultShapesVisible(true);
        renderer.setDefaultShapesFilled(true);
        renderer.setDefaultStroke(new BasicStroke(2.5f));
        
        // Use a nice font for the title
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 18));
    }
}
