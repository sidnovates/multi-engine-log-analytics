package Pig.UDF;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import common.Parsing.LogParser;
import common.Parsing.ParsedLog;

public class LogParserPigUDF extends EvalFunc<Tuple> {

    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }

        ParsedLog log = LogParser.parse(input.get(0).toString());
        if (log == null) {
            return null;
        }

        Tuple out = TUPLE_FACTORY.newTuple(8);
        try {
            out.set(0, log.host);
            out.set(1, log.logDate);
            out.set(2, Integer.parseInt(log.logHour));
            out.set(3, log.method);
            out.set(4, log.resource);
            out.set(5, log.protocol);
            out.set(6, log.statusCode);
            out.set(7, (long) log.bytes);
            return out;
        } catch (Exception e) {
            return null;
        }
    }
}
