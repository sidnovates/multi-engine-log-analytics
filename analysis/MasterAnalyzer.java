package analysis;

import java.io.File;

public class MasterAnalyzer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java analysis.MasterAnalyzer <execution_id>");
            return;
        }

        int executionId = Integer.parseInt(args[0]);
        System.out.println("======================================================================");
        System.out.println("                       MASTER ANALYSER TRIGGER                        ");
        System.out.println("======================================================================");
        System.out.println("Triggering analysis for Execution ID: " + executionId);

        // 1. Data Quality Analysis
        System.out.println("\n[STEP 1] Running Data Quality Analysis...");
        DataQualityAnalyzer.analyzeDataQuality(executionId);

        // 2. Pipeline Comparison Analysis
        System.out.println("\n[STEP 2] Running Pipeline Comparison Analysis...");
        PipelineComparisonAnalyzer.analyzePipelineComparison(executionId);

        // 3. Performance Analysis
        System.out.println("\n[STEP 3] Running Performance Analysis...");
        QueryTimingAnalyzer.analyzePerformance(executionId);

        // 4. Batch Size Impact Analysis
        System.out.println("\n[STEP 4] Running Batch Size Impact Analysis...");
        BatchSizeImpactAnalyzer.analyzeBatchImpact(executionId);

        System.out.println("\n======================================================================");
        System.out.println("                   ALL ANALYSES COMPLETED SUCCESSFULLY                ");
        System.out.println("======================================================================");
    }
}
