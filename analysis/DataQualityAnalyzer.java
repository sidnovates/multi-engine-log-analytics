package analysis;

import common.sql.DBConnection;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataQualityAnalyzer {

    private static final String GRAPH_DIR = "analysis/graphs/DataQuality/";

    public static void main(String[] args) {
        if (args.length < 1) return;
        int executionId = Integer.parseInt(args[0]);

        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) dir.mkdirs();

        analyzeDataQuality(executionId);
    }

    public static void analyzeDataQuality(int runId) {
        String datasetName = "";
        String baselinePipeline = "";
        long baseTotal = 0, baseMalformed = 0;

        String sql = "SELECT dataset_name, pipeline_name, total_record_count, total_malformed_record_count " +
                     "FROM run_metadata WHERE run_id = ?";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, runId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                datasetName = rs.getString("dataset_name");
                baselinePipeline = rs.getString("pipeline_name").toLowerCase();
                baseTotal = rs.getLong("total_record_count");
                baseMalformed = rs.getLong("total_malformed_record_count");
            } else {
                return;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        Map<String, long[]> engineData = new LinkedHashMap<>();
        String[] engines = {"mr", "pig", "hive", "mongodb"};

        for (String engine : engines) {
            if (engine.equals(baselinePipeline)) {
                engineData.put(engine, new long[]{baseTotal, baseMalformed, runId});
            } else {
                long[] hist = getLatestSuccessfulRun(datasetName, engine);
                engineData.put(engine, hist);
            }
        }

        generateMultiPieChartDashboard(runId, datasetName, baselinePipeline, engineData);
    }

    private static long[] getLatestSuccessfulRun(String datasetName, String engine) {
        String sql = "SELECT run_id, total_record_count, total_malformed_record_count FROM run_metadata " +
                     "WHERE dataset_name = ? AND pipeline_name = ? AND total_runtime > 0 " +
                     "AND total_malformed_record_count < total_record_count " +
                     "ORDER BY execution_timestamp DESC LIMIT 1";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, datasetName);
            pstmt.setString(2, engine);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new long[]{
                    rs.getLong("total_record_count"),
                    rs.getLong("total_malformed_record_count"),
                    rs.getLong("run_id")
                };
            }
        } catch (SQLException e) {}
        return null;
    }

    private static void generateMultiPieChartDashboard(int executionId, String datasetName, String baselinePipeline, Map<String, long[]> engineData) {
        int width = 1000;
        int height = 800;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = img.createGraphics();

        // Background
        g2d.setColor(Color.WHITE);
        g2d.fillRect(0, 0, width, height);
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        // Main Title
        g2d.setColor(new Color(33, 37, 41));
        g2d.setFont(new Font("SansSerif", Font.BOLD, 30));
        String title = "Data Quality Integrity Comparison - " + datasetName;
        FontMetrics fm = g2d.getFontMetrics();
        g2d.drawString(title, (width - fm.stringWidth(title)) / 2, 45);

        int gridW = width / 2;
        int gridH = (height - 80) / 2;
        int startY = 80;

        String[] enginesToPrint = {"mr", "pig", "hive", "mongodb"};
        
        for (int i = 0; i < enginesToPrint.length; i++) {
            String eng = enginesToPrint[i];
            long[] data = engineData.get(eng);
            
            int x = (i % 2) * gridW;
            int y = startY + ((i / 2) * gridH);
            Rectangle2D area = new Rectangle2D.Double(x + 20, y + 20, gridW - 40, gridH - 40);

            if (data == null) {
                drawEmptyPlaceholder(g2d, eng, area);
            } else {
                long total = data[0];
                long malformed = data[1];
                long valid = total - malformed;
                long runId = data[2];
                boolean isBaseline = eng.equals(baselinePipeline);

                JFreeChart chart = createPieChart(eng, valid, malformed, isBaseline, runId);
                chart.draw(g2d, area);
            }
        }

        g2d.dispose();

        File chartFile = new File(GRAPH_DIR + "quality_chart_" + executionId + ".png");
        try {
            ImageIO.write(img, "png", chartFile);
            System.out.println("-> Quality Comparison Dashboard saved to: " + chartFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JFreeChart createPieChart(String engine, long valid, long malformed, boolean isBaseline, long runId) {
        DefaultPieDataset dataset = new DefaultPieDataset();
        dataset.setValue("Valid", valid);
        dataset.setValue("Malformed", malformed);

        String title = engine.toUpperCase();
        if (isBaseline) title += " (BASELINE RUN)";
        else title += " (Latest: Run ID " + runId + ")";

        JFreeChart chart = ChartFactory.createPieChart(title, dataset, true, true, false);
        chart.setBackgroundPaint(Color.WHITE);
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 20));
        
        if (isBaseline) {
            chart.getTitle().setPaint(new Color(23, 162, 184)); // Cyan
        } else {
            chart.getTitle().setPaint(new Color(33, 37, 41)); // Dark
        }

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setOutlineVisible(false);
        plot.setShadowPaint(null); 

        plot.setSectionPaint("Valid", new Color(46, 204, 113)); // Emerald Green
        plot.setSectionPaint("Malformed", new Color(231, 76, 60)); // Vibrant Red

        plot.setLabelGenerator(new org.jfree.chart.labels.StandardPieSectionLabelGenerator("{0}: {1}"));
        plot.setLabelFont(new Font("SansSerif", Font.BOLD, 14));
        plot.setLabelBackgroundPaint(new Color(248, 249, 250));
        plot.setLabelOutlinePaint(null);
        plot.setLabelShadowPaint(null);

        chart.getLegend().setFrame(org.jfree.chart.block.BlockBorder.NONE);
        chart.getLegend().setItemFont(new Font("SansSerif", Font.PLAIN, 14));
        return chart;
    }

    private static void drawEmptyPlaceholder(Graphics2D g2d, String engine, Rectangle2D area) {
        g2d.setColor(new Color(248, 249, 250));
        g2d.fill(area);
        
        g2d.setColor(new Color(206, 212, 218));
        g2d.draw(area);

        g2d.setColor(new Color(108, 117, 125));
        g2d.setFont(new Font("SansSerif", Font.BOLD, 24));
        
        String text = engine.toUpperCase() + " - NO HISTORY";
        FontMetrics fm = g2d.getFontMetrics();
        int x = (int) (area.getX() + (area.getWidth() - fm.stringWidth(text)) / 2);
        int y = (int) (area.getY() + (area.getHeight() / 2));
        g2d.drawString(text, x, y);
    }
}
