package parser.query;

import java.text.ParseException;
import java.util.Set;

public interface QLQuery {
    Set<Object> execute(String query) throws ParseException;
}