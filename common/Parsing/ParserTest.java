package common.Parsing;
import java.io.BufferedReader;
import java.io.FileReader;

public class ParserTest {

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Usage: java ParserTest <log_file>");
            return;
        }

        String filePath = args[0];

        int validCount = 0;
        int malformedCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            String line;

            while ((line = br.readLine()) != null) {

                ParsedLog log = LogParser.parse(line);

                if (log == null) {
                    malformedCount++;
                    continue;
                }

                validCount++;

                // 🔹 Print parsed output (for checking)
                System.out.println(
                        "Date: " + log.logDate +
                        " | Hour: " + log.logHour +
                        " | Status: " + log.statusCode +
                        " | Bytes: " + log.bytes
                );
            }

            System.out.println("\n===== SUMMARY =====");
            System.out.println("Valid Records: " + validCount);
            System.out.println("Malformed Records: " + malformedCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}