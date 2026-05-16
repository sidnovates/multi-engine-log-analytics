package common.Parsing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {

    // ✅ Correct and safe regex
    private static final Pattern logPattern = Pattern.compile(
        "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+(HTTP/[\\d.]+)\"\\s+(\\d{3})\\s+(\\d+|-)$"
    );

    public static ParsedLog parse(String line) {

        Matcher matcher = logPattern.matcher(line);

        // ❌ malformed record
        if (!matcher.matches()) {
            return null;
        }
        try {
            String host = matcher.group(1);
            String logDate = matcher.group(2);
            String logHour = matcher.group(3);
            String method = matcher.group(4);
            String resource = matcher.group(5);
            String protocol = matcher.group(6);

            int statusCode = Integer.parseInt(matcher.group(7));

            String bytesStr = matcher.group(8);
            int bytes = bytesStr.equals("-") ? 0 : Integer.parseInt(bytesStr);

            return new ParsedLog(host, logDate, logHour,
                    method, resource, protocol,
                    statusCode, bytes);

        } catch (Exception e) {
            return null;
        }
    }
}