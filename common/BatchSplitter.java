package common;
import java.io.*;

public class BatchSplitter {

    public static void splitFile(String inputFile, int batchSize) {
        int batchId = 1;
        int totalLineCount = 0;

        BufferedReader reader = null;
        BufferedWriter writer = null;
        File currentBatchFile = null;
        int currentBatchLineCount = 0;

        try {
            File inputFileObj = new File(inputFile);
            String inputFileName = inputFileObj.getName();
            String nameWithoutExtension = inputFileName.contains(".") ? 
                                          inputFileName.substring(0, inputFileName.lastIndexOf('.')) : 
                                          inputFileName;
            String baseDirName = nameWithoutExtension + "_" + batchSize;
            
            // 🔹 Create Dataset directory inside common folder
            File datasetDir = new File("common/Dataset");
            if (!datasetDir.exists()) {
                datasetDir.mkdirs();
            }
            
            File outputDir = new File(datasetDir, baseDirName);
            if (!outputDir.exists()) {
                outputDir.mkdirs();
                System.out.println("Created directory: " + outputDir.getPath());
            }

            reader = new BufferedReader(new FileReader(inputFile));
            String line;

            while ((line = reader.readLine()) != null) {
                // 🔹 Start a new batch file if needed
                if (currentBatchLineCount == 0) {
                    // Use a temporary name first, then rename to include final count
                    currentBatchFile = new File(outputDir, "temp_b" + batchId + ".txt");
                    writer = new BufferedWriter(new FileWriter(currentBatchFile));
                    System.out.println("Creating batch " + batchId + "...");
                }

                writer.write(line);
                writer.newLine();
                currentBatchLineCount++;
                totalLineCount++;

                // 🔹 Close batch file if it reaches batchSize
                if (currentBatchLineCount == batchSize) {
                    writer.close();
                    writer = null;
                    
                    // Rename temp file to include final count
                    File finalFile = new File(outputDir, "b" + batchId + "_" + currentBatchLineCount + ".txt");
                    currentBatchFile.renameTo(finalFile);
                    
                    batchId++;
                    currentBatchLineCount = 0;
                }
            }

            // 🔹 Handle the last batch if it has remaining lines
            if (writer != null) {
                writer.close();
                File finalFile = new File(outputDir, "b" + batchId + "_" + currentBatchLineCount + ".txt");
                currentBatchFile.renameTo(finalFile);
            }

            System.out.println("\nTotal Records Processed: " + totalLineCount);
            System.out.println("Total Batches Created: " + (currentBatchLineCount > 0 ? batchId : batchId - 1));
            System.out.println("Output location: " + outputDir.getAbsolutePath());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) reader.close();
                if (writer != null) writer.close();
            } catch (IOException e) {}
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java common.BatchSplitter <input_file> <batch_size>");
            return;
        }

        String inputFile = args[0];
        int batchSize;
        try {
            batchSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: Batch size must be an integer.");
            return;
        }

        splitFile(inputFile, batchSize);
    }
}