package common.Parsing;

public class ParsedLog {

    public String host;
    public String logDate;
    public String logHour;
    public String method;
    public String resource;
    public String protocol;
    public int statusCode;
    public int bytes;

    public ParsedLog(String host, String logDate, String logHour,
                     String method, String resource, String protocol,
                     int statusCode, int bytes) {

        this.host = host;
        this.logDate = logDate;
        this.logHour = logHour;
        this.method = method;
        this.resource = resource;
        this.protocol = protocol;
        this.statusCode = statusCode;
        this.bytes = bytes;
    }
}