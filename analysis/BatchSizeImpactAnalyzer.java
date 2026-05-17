package analysis;

import common.sql.DBConnection;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

public class BatchSizeImpactAnalyzer {

    private static final String GRAPH_DIR = "analysis/graphs/BatchSizeAnalyzer/";

    public static void analyzeBatchImpact(int executionId) {
        String datasetName = "";
        String engineName = "";
        
        String metaSql = "SELECT dataset_name, pipeline_name FROM run_metadata WHERE run_id = ? LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(metaSql)) {
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                datasetName = rs.getString("dataset_name");
                engineName = rs.getString("pipeline_name");
            } else {
                System.out.println("No metadata found for run_id: " + executionId);
                return;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) dir.mkdirs();

        String baseName = datasetName;
        if (datasetName.contains("_")) {
            baseName = datasetName.substring(0, datasetName.lastIndexOf('_'));
        }
        String likePattern = baseName + "\\_%"; 

        try (Connection conn = DBConnection.getConnection()) {
            System.out.println("Generating Batch Size Impact Analysis for base dataset: " + baseName + " using " + engineName.toUpperCase());

            generateRuntimeGraph(conn, likePattern, engineName, baseName, executionId);
            generateAvgBatchTimeGraph(conn, likePattern, engineName, baseName, executionId);

            System.out.println("Batch Size Impact Graphs generated successfully in " + GRAPH_DIR);
        } catch (SQLException e) {
            System.err.println("SQL Error during Batch Size Impact Analysis: " + e.getMessage());
        }
    }

    private static void generateRuntimeGraph(Connection conn, String likePattern, String engineName, String baseName, int executionId) throws SQLException {
        String sql = "SELECT DISTINCT ON (dataset_name) dataset_name, total_runtime " +
                     "FROM (" +
                     "  SELECT dataset_name, total_runtime, execution_timestamp, run_id FROM run_metadata WHERE run_id = ?" +
                     "  UNION ALL " +
                     "  SELECT dataset_name, total_runtime, execution_timestamp, run_id FROM run_metadata " +
                     "  WHERE dataset_name LIKE ? AND pipeline_name = ? AND run_id != ? " +
                     "    AND total_runtime > 0 AND total_record_count > 1000" +
                     ") sub " +
                     "ORDER BY dataset_name, (run_id = ?) DESC, execution_timestamp DESC";

        TreeMap<Integer, Double> sortedData = new TreeMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            pstmt.setString(2, likePattern);
            pstmt.setString(3, engineName.toLowerCase());
            pstmt.setInt(4, executionId);
            pstmt.setInt(5, executionId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String dsName = rs.getString("dataset_name");
                double time = rs.getDouble("total_runtime");
                try {
                    int batchSize = Integer.parseInt(dsName.substring(dsName.lastIndexOf('_') + 1));
                    sortedData.put(batchSize, time);
                } catch (Exception e) {
                    continue;
                }
            }
        }
        
        if (!sortedData.isEmpty()) {
            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
            for (Map.Entry<Integer, Double> entry : sortedData.entrySet()) {
                dataset.addValue(entry.getValue(), "Total Runtime", String.valueOf(entry.getKey()));
            }
            saveLineChart(dataset, "Batch Size vs Total Runtime", "Target Batch Size", "Total Runtime (s)", "batch_size_runtime_" + baseName.replace("/", "_") + ".png");
        }
    }



    private static void generateAvgBatchTimeGraph(Connection conn, String likePattern, String engineName, String baseName, int executionId) throws SQLException {
        String sql = "SELECT DISTINCT ON (dataset_name) dataset_name, avg_batch_time " +
                     "FROM (" +
                     "  SELECT dataset_name, (SELECT AVG(batch_runtime) FROM batch_metadata WHERE run_id = rm.run_id) as avg_batch_time, execution_timestamp, run_id " +
                     "  FROM run_metadata rm WHERE run_id = ?" +
                     "  UNION ALL " +
                     "  SELECT dataset_name, (SELECT AVG(batch_runtime) FROM batch_metadata WHERE run_id = rm.run_id) as avg_batch_time, execution_timestamp, run_id " +
                     "  FROM run_metadata rm " +
                     "  WHERE dataset_name LIKE ? AND pipeline_name = ? AND run_id != ? " +
                     "    AND total_runtime > 0 AND total_record_count > 1000" +
                     ") sub " +
                     "ORDER BY dataset_name, (run_id = ?) DESC, execution_timestamp DESC";

        TreeMap<Integer, Double> sortedData = new TreeMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, executionId);
            pstmt.setString(2, likePattern);
            pstmt.setString(3, engineName.toLowerCase());
            pstmt.setInt(4, executionId);
            pstmt.setInt(5, executionId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String dsName = rs.getString("dataset_name");
                double time = rs.getDouble("avg_batch_time");
                try {
                    int batchSize = Integer.parseInt(dsName.substring(dsName.lastIndexOf('_') + 1));
                    sortedData.put(batchSize, time);
                } catch (Exception e) {
                    continue;
                }
            }
        }
        
        if (!sortedData.isEmpty()) {
            DefaultCategoryDataset dataset = new DefaultCategoryDataset();
            for (Map.Entry<Integer, Double> entry : sortedData.entrySet()) {
                dataset.addValue(entry.getValue(), "Avg Batch Time", String.valueOf(entry.getKey()));
            }
            saveLineChart(dataset, "Batch Size vs Average Batch Time", "Target Batch Size", "Time per Batch (s)", "batch_size_avgtime_" + baseName.replace("/", "_") + ".png");
        }
    }

    private static void saveLineChart(DefaultCategoryDataset dataset, String title, String xLabel, String yLabel, String fileName) {
        JFreeChart chart = ChartFactory.createLineChart(
                title, xLabel, yLabel, dataset,
                PlotOrientation.VERTICAL, true, true, false);
        styleAndSaveChart(chart, fileName);
    }

    private static void saveBarChart(DefaultCategoryDataset dataset, String title, String xLabel, String yLabel, String fileName) {
        JFreeChart chart = ChartFactory.createBarChart(
                title, xLabel, yLabel, dataset,
                PlotOrientation.VERTICAL, true, true, false);
        styleAndSaveChart(chart, fileName);
    }

    private static void styleAndSaveChart(JFreeChart chart, String fileName) {
        chart.setBackgroundPaint(Color.WHITE);
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 22));
        chart.getTitle().setPaint(new Color(33, 37, 41)); // Dark modern text
        
        // Enhance plot area
        chart.getCategoryPlot().setBackgroundPaint(new Color(248, 249, 250)); // Very light gray
        chart.getCategoryPlot().setRangeGridlinePaint(new Color(206, 212, 218));
        chart.getCategoryPlot().setOutlineVisible(false); // Remove border

        // Thicken lines if it's a line chart
        if (chart.getCategoryPlot().getRenderer() instanceof org.jfree.chart.renderer.category.LineAndShapeRenderer) {
            org.jfree.chart.renderer.category.LineAndShapeRenderer renderer = (org.jfree.chart.renderer.category.LineAndShapeRenderer) chart.getCategoryPlot().getRenderer();
            renderer.setSeriesStroke(0, new BasicStroke(3.5f)); // Thicker lines
            renderer.setSeriesPaint(0, new Color(79, 70, 229)); // Modern indigo
            renderer.setSeriesShapesVisible(0, true); // Show data dots
            renderer.setUseSeriesOffset(false);

            // Show values at the data points
            renderer.setDefaultItemLabelsVisible(true);
            renderer.setDefaultItemLabelGenerator(new StandardCategoryItemLabelGenerator());
            renderer.setDefaultItemLabelFont(new Font("SansSerif", Font.BOLD, 12));
        }
        
        // Style bars if it's a bar chart
        if (chart.getCategoryPlot().getRenderer() instanceof org.jfree.chart.renderer.category.BarRenderer) {
            org.jfree.chart.renderer.category.BarRenderer renderer = (org.jfree.chart.renderer.category.BarRenderer) chart.getCategoryPlot().getRenderer();
            renderer.setSeriesPaint(0, new Color(16, 185, 129)); // Modern emerald
            renderer.setShadowVisible(false);
            renderer.setDrawBarOutline(false);
            renderer.setItemMargin(0.1);

            // Show values at the top of the bars
            renderer.setDefaultItemLabelsVisible(true);
            renderer.setDefaultItemLabelGenerator(new StandardCategoryItemLabelGenerator());
            renderer.setDefaultItemLabelFont(new Font("SansSerif", Font.BOLD, 12));
        }

        try {
            ChartUtils.saveChartAsPNG(new File(GRAPH_DIR + fileName), chart, 900, 600); // slightly wider for better scale
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
