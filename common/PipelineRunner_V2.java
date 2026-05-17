package common;

import java.io.*;
import java.util.*;
import common.sql.MetadataDAO;

public class PipelineRunner_V2 {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String datasetPath = "common/Dataset";
        File datasetDir = new File(datasetPath);

        if (!datasetDir.exists() || !datasetDir.isDirectory()) {
            System.out.println("Error: Dataset directory not found at " + datasetPath);
            return;
        }

        File selectedFolder = null;
        String engineName = null;
        String classSuffix = null;
        int targetBatchSize = 0;

        if (args.length >= 2) {
            // Headless Mode (from Frontend)
            String selectedFolderName = args[0];
            selectedFolder = new File(datasetDir, selectedFolderName);
            if (!selectedFolder.exists() || !selectedFolder.isDirectory()) {
                System.out.println("Error: Dataset folder not found: " + selectedFolderName);
                return;
            }
            String engineArg = args[1].toLowerCase();
            if (engineArg.equals("mongodb")) {
                engineName  = "MongoDB";
                classSuffix = "Mongo";
            } else if (engineArg.equals("hive")) {
                engineName  = "Hive";
                classSuffix = "Hive";
            } else if (engineArg.equals("pig")) {
                engineName  = "Pig";
                classSuffix = "Pig";
            } else {
                engineName  = "MR";
                classSuffix = "MR";
            }
            if (args.length >= 3) {
                try {
                    targetBatchSize = Integer.parseInt(args[2]);
                } catch (Exception e) {}
            }
        } else {
            // Interactive Mode (CLI)
            File[] folders = datasetDir.listFiles(File::isDirectory);
            if (folders == null || folders.length == 0) {
                System.out.println("No batch folders found in " + datasetPath);
                return;
            }

            System.out.println("Available Batch Folders:");
            for (int i = 0; i < folders.length; i++) {
                System.out.println("[" + (i + 1) + "] " + folders[i].getName());
            }

            System.out.print("Select a folder (index): ");
            int folderIndex = scanner.nextInt() - 1;
            if (folderIndex < 0 || folderIndex >= folders.length) {
                System.out.println("Invalid selection.");
                return;
            }
            selectedFolder = folders[folderIndex];

            // 2. Select Engine
            System.out.println("\nSelect Processing Engine:");
            System.out.println("[1] MongoDB");
            System.out.println("[2] MR (MapReduce)");
            System.out.println("[3] Hive");
            System.out.println("[4] Pig");
            System.out.print("Choice: ");
            int engineChoice = scanner.nextInt();
            if (engineChoice == 1) {
                engineName = "MongoDB"; classSuffix = "Mongo";
            } else if (engineChoice == 3) {
                engineName = "Hive";    classSuffix = "Hive";
            } else if (engineChoice == 4) {
                engineName = "Pig";     classSuffix = "Pig";
            } else {
                engineName = "MR";      classSuffix = "MR";
            }
        }

        // 3. Compile Everything Once (Automatic)
        System.out.println("\n--- Running Maven Compile ---");
        runCommand("mvn compile");

        if (engineName.equalsIgnoreCase("MR")) {
            System.out.println("\n--- Pre-compiling MapReduce JARs ---");
            runCommand("javac -classpath \"$(cat .classpath):`hadoop classpath`\" -d . common/Parsing/*.java common/sql/*.java common/MR/DataIngestionMR.java Query1/MR/DailyTrafficMR_V2.java Query2/MR/TopResourcesMR_V2.java Query3/MR/HourlyErrorMR_V2.java");
            runCommand("jar -cf common/MR/DataIngestionMR.jar common/Parsing/*.class common/sql/*.class common/MR/*.class");
            runCommand("jar -cf Query1/MR/DailyTrafficMR_V2.jar common/Parsing/*.class common/sql/*.class Query1/MR/*.class");
            runCommand("jar -cf Query2/MR/TopResourcesMR_V2.jar common/Parsing/*.class common/sql/*.class Query2/MR/*.class");
            runCommand("jar -cf Query3/MR/HourlyErrorMR_V2.jar common/Parsing/*.class common/sql/*.class Query3/MR/*.class");
        } else if (engineName.equalsIgnoreCase("Pig")) {
            System.out.println("\n--- Pre-compiling Pig UDF JAR ---");
            runCommand("mkdir -p /tmp/pig_udf_classes");
            runCommand("javac -classpath \"$(cat .classpath):`hadoop classpath`\" -d /tmp/pig_udf_classes common/Parsing/*.java Pig/UDF/LogParserPigUDF.java");
            runCommand("jar -cf Pig/UDF/logparser-udf.jar -C /tmp/pig_udf_classes .");
        }

        // 4. Load Batches
        File[] batchFiles = selectedFolder.listFiles((dir, name) -> name.endsWith(".txt") && !name.startsWith("temp_"));
        
        if ((batchFiles == null || batchFiles.length == 0) && selectedFolder.getName().startsWith("upload_")) {
            // Check if it's a custom upload that needs splitting
            File[] files = selectedFolder.listFiles();
            if (files != null && files.length > 0 && targetBatchSize > 0) {
                System.out.println("[AUTO-SPLIT] Custom upload detected. Splitting file: " + files[0].getName());
                BatchSplitter.splitFile(files[0].getPath(), targetBatchSize);
                
                // Switch to the newly created folder (matches BatchSplitter logic)
                String name = files[0].getName();
                String nameWithoutExt = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
                selectedFolder = new File(datasetDir, nameWithoutExt + "_" + targetBatchSize);
                
                // Re-scan for batches in the new folder
                batchFiles = selectedFolder.listFiles((dir, n) -> n.endsWith(".txt") && !n.startsWith("temp_"));
            }
        }

        if (batchFiles == null || batchFiles.length == 0) {
            System.out.println("No batch files found in " + selectedFolder.getPath());
            return;
        }
        Arrays.sort(batchFiles, Comparator.comparing(File::getName));

        // 5. Calculate Stats
        long totalRecordsInFolder = 0;
        int nonEmptyBatches = 0; 
        if (batchFiles != null) nonEmptyBatches = batchFiles.length;

        if (targetBatchSize <= 0) {
            try {
                String folderName = selectedFolder.getName();
                targetBatchSize = Integer.parseInt(folderName.substring(folderName.lastIndexOf('_') + 1));
            } catch (Exception e) {}
        }

        for (File f : batchFiles) {
            String name = f.getName();
            try {
                int underscoreIndex = name.lastIndexOf('_');
                int dotIndex = name.lastIndexOf('.');
                totalRecordsInFolder += Integer.parseInt(name.substring(underscoreIndex + 1, dotIndex));
            } catch (Exception e) {}
        }
        double avgBatchSize = nonEmptyBatches > 0 ? (double) totalRecordsInFolder / nonEmptyBatches : 0;
        
        System.out.println("\nDataset Statistics:");
        System.out.println("Target Batch Size: " + targetBatchSize);
        System.out.println("Total Records: " + totalRecordsInFolder);
        System.out.println("Total Batches: " + nonEmptyBatches);
        System.out.printf("Average Batch Size: %.2f\n", avgBatchSize);

        System.out.println("\n--- Starting Pipeline Execution (All Queries) ---");
        int runId = MetadataDAO.insertRunMetadata(engineName, selectedFolder.getName(), avgBatchSize);
        System.out.println("Execution ID for this full run: " + runId);
        
        // Save execution ID for the UI/Analyzer to pick up
        try (PrintWriter out = new PrintWriter(".latest_execution_id")) {
            out.println(runId);
        } catch (Exception e) {}
        
        long totalPipelineRecords = 0;
        double totalPipelineRuntimeMs = 0;

        for (File batchFile : batchFiles) {
            String fileName = batchFile.getName();
            System.out.println("\nProcessing Batch: " + fileName);

            int batchNo = 0;
            int currentBatchSize = 0;
            try {
                int bIndex = fileName.indexOf('b');
                int underscoreIndex = fileName.lastIndexOf('_');
                int dotIndex = fileName.lastIndexOf('.');
                batchNo = Integer.parseInt(fileName.substring(bIndex + 1, underscoreIndex));
                currentBatchSize = Integer.parseInt(fileName.substring(underscoreIndex + 1, dotIndex));
            } catch (Exception e) {}

            totalPipelineRecords += currentBatchSize;
            MetadataDAO.insertBatchMetadata(runId, batchNo, currentBatchSize);

            System.out.println("Processing Batch No: " + batchNo);

            long batchStart = System.currentTimeMillis();
            
            // 🔹 MR PHASE 1: INGESTION STAGE (Run exactly once per batch)
            String queryInputPath = batchFile.getPath();
            String tsvOutputDir = null;
            if (engineName.equalsIgnoreCase("MR")) {
                tsvOutputDir = "parsed_tsv_run" + runId + "_batch" + batchNo;
                System.out.println("Running Phase 1: MapReduce Ingestion to TSV...");
                runMRIngestion(batchFile.getPath(), tsvOutputDir, runId);
                queryInputPath = tsvOutputDir; // Point Q1, Q2, Q3 to the pre-parsed TSV folder
            } else if (engineName.equalsIgnoreCase("MongoDB")) {
                System.out.println("Running Phase 1: MongoDB Raw Text Ingestion (Native JVM)...");
                common.MongoDB.DataIngestionMongo.run(batchFile.getPath(), runId);
            } else if (engineName.equalsIgnoreCase("Pig")) {
                System.out.println("Running Phase 1: Pig TSV Ingestion (Native JVM)...");
                try {
                    common.PIG.DataIngestionPig.run(batchFile.getPath(), runId);
                    queryInputPath = "/user/nasa_etl/pig_parsed/batch_" + runId;
                } catch (Exception e) { e.printStackTrace(); }
            }

            // Execute All Queries (1, 2, and 3)
            for (int q = 1; q <= 3; q++) {
                long qStart = System.currentTimeMillis();
                
                if (engineName.equalsIgnoreCase("MongoDB")) {
                    System.out.println("Executing Query " + q + " (MongoDB V2 Phase 2 natively)...");
                    if (q == 1) Query1.MongoDB.Q1Mongo_V2.run(runId);
                    else if (q == 2) Query2.MongoDB.Q2Mongo_V2.run(runId);
                    else if (q == 3) Query3.MongoDB.Q3Mongo_V2.run(runId);
                } else if (engineName.equalsIgnoreCase("Pig")) {
                    System.out.println("Executing Query " + q + " (Pig V2 Phase 2 natively)...");
                    try {
                        if (q == 1) Query1.PIG.Q1Pig_V2.run(queryInputPath, runId);
                        else if (q == 2) Query2.PIG.Q2Pig_V2.run(queryInputPath, runId);
                        else if (q == 3) Query3.PIG.Q3Pig_V2.run(queryInputPath, runId);
                    } catch (Exception e) { e.printStackTrace(); }
                } else {
                    runQuery(q, engineName, classSuffix, queryInputPath, runId);
                }

                long qDuration = System.currentTimeMillis() - qStart;

                MetadataDAO.saveQueryMetadata(runId, batchNo, q, qDuration);
            }
            
            // 🔹 CLEANUP: Delete TSV directory after all queries are done
            if (engineName.equalsIgnoreCase("MR") && tsvOutputDir != null) {
                System.out.println("Cleaning up parsed TSV directory: " + tsvOutputDir);
                runCommand("rm -rf " + tsvOutputDir); // Clean up local file system (using local file path semantics of MR)
            } else if (engineName.equalsIgnoreCase("Pig")) {
                System.out.println("Cleaning up parsed HDFS TSV directory for Pig...");
                runCommand("hadoop fs -rm -r -skipTrash " + queryInputPath);
            }
            
            long batchEnd = System.currentTimeMillis();
            long batchDurationMs = batchEnd - batchStart;
            totalPipelineRuntimeMs += batchDurationMs;
            
            MetadataDAO.updateBatchStats(runId, batchNo, batchDurationMs / 1000.0, 0);
        }
        
        // Finalize run metadata
        MetadataDAO.updateFinalStats(runId, totalPipelineRuntimeMs / 1000.0, 0, totalPipelineRecords);
        
        if (engineName.equalsIgnoreCase("MongoDB")) {
            common.MongoDB.MongoConnectionManager.close();
        }
        
        System.out.println("\n--- Pipeline Execution Completed ---");
    }

    private static void runQuery(int queryNum, String engine, String suffix, String filePath, int runId) {
        if (engine.equalsIgnoreCase("MongoDB")) {
            runMongoQuery(queryNum, engine, suffix, filePath, runId);
        } else if (engine.equalsIgnoreCase("Hive")) {
            runHiveQuery(queryNum, filePath, runId);
        } else if (engine.equalsIgnoreCase("Pig")) {
            runPigQuery(queryNum, filePath, runId);
        } else {
            runMRQuery(queryNum, filePath, runId);
        }
    }

    private static void runHiveQuery(int queryNum, String filePath, int runId) {
        String queryName = (queryNum == 1) ? "Q1Hive" : (queryNum == 2) ? "Q2Hive" : "Q3Hive";
        String mainClass = "Query" + queryNum + ".Hive." + queryName;
        String cp = "/home/hitan/DBMS/compiled:$(hadoop classpath):/home/hitan/hive/lib/*:/home/hitan/.m2/repository/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar";
        String command = "java -Xmx512m -cp \"" + cp + "\" " + mainClass + " \"" + filePath + "\" " + runId;
        System.out.println("Executing Query " + queryNum + " (Hive)...");
        runCommand(command);
    }

    private static void runPigQuery(int queryNum, String filePath, int runId) {
        String queryName = (queryNum == 1) ? "Q1Pig" : (queryNum == 2) ? "Q2Pig" : "Q3Pig";
        String mainClass = "Query" + queryNum + ".PIG." + queryName;
        // Use the same classpath as Maven compile output; hadoop classpath provides HDFS client jars
        String cp = "target/classes:$(hadoop classpath):$(find $HOME/.m2 -name 'postgresql-*.jar' 2>/dev/null | head -1)";
        String command = "java -Xmx512m -cp \"" + cp + "\" " + mainClass + " \"" + filePath + "\" " + runId;
        System.out.println("Executing Query " + queryNum + " (Pig)...");
        runCommand(command);
    }

    
    // // 🔹 NEW INGESTION METHOD for MongoDB Phase 1
    // private static void runMongoIngestion(String filePath, int runId) {
    //     String mainClass = "common.MongoDB.DataIngestionMongo";
    //     String command = "mvn exec:java -Dexec.mainClass=\"" + mainClass + "\" -Dexec.args=\"" + filePath + " " + runId + "\"";
    //     runCommand(command);
    // }
    private static void runMongoQuery(int queryNum, String engine, String suffix, String filePath, int runId) {
        String mainClass = "Query" + queryNum + "." + engine + ".Q" + queryNum + suffix + "_V2";
        String command = "mvn exec:java -Dexec.mainClass=\"" + mainClass + "\" -Dexec.args=\"" + filePath + " " + runId + "\"";
        System.out.println("Executing Query " + queryNum + " (MongoDB V2 Phase 2)...");
        runCommand(command);
    }
    
    // 🔹 NEW INGESTION METHOD for MR Phase 1
    private static void runMRIngestion(String inputPath, String tsvOutputDir, int runId) {
        String mainClass = "common.MR.DataIngestionMR";
        String jarPath = "common/MR/DataIngestionMR.jar";
        
        File inputFile = new File(inputPath);
        String absInputPath = "file://" + (inputFile.getAbsolutePath().startsWith("/") ? "" : "/") + inputFile.getAbsolutePath();
        String absOutputPath = "file://" + new File(tsvOutputDir).getAbsolutePath();

        System.out.println("Executing Data Ingestion (MapReduce V2 Phase 1)...");

        runCommand("hadoop jar " + jarPath + " " + mainClass + " " + absInputPath + " " + absOutputPath + " " + runId);
    }

    // 🔹 MODIFIED MR QUERY METHOD to use _V2 Classes and TSV input
    private static void runMRQuery(int queryNum, String inputPath, int runId) {
        String queryName = (queryNum == 1) ? "DailyTrafficMR_V2" : (queryNum == 2) ? "TopResourcesMR_V2" : "HourlyErrorMR_V2";
        String mainClass = "Query" + queryNum + ".MR." + queryName;

        // Note: inputPath here is already the TSV directory from Phase 1
        String absInputPath = "file://" + new File(inputPath).getAbsolutePath();
        
        File outputFile = new File("output_Q" + queryNum + "_run" + runId);
        String absOutputPath = "file://" + (outputFile.getAbsolutePath().startsWith("/") ? "" : "/") + outputFile.getAbsolutePath();
        String jarPath = "Query" + queryNum + "/MR/" + queryName + ".jar";

        System.out.println("Executing Query " + queryNum + " (MapReduce V2 Phase 2)...");

        // 3. Run Hadoop
        runCommand("hadoop jar " + jarPath + " " + mainClass + " " + absInputPath + " " + absOutputPath + " " + runId);
    }

    private static void runCommand(String command) {
        try {
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb.redirectErrorStream(true);
            // Redirect input from /dev/null to prevent the terminal from suspending the process (SIGTTIN)
            pb.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")));
            
            Process process = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                // Only print important Maven/Hadoop lines to keep output clean
                if (line.contains("[INFO] Building") || line.contains("BUILD SUCCESS") || line.contains("Step") || line.contains("Total Pipeline Runtime")) {
                    System.out.println(line);
                }
            }
            process.waitFor();
        } catch (Exception e) {}
    }
}
