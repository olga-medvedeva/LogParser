package parser;

import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Date;

public class Solution {
    public static void main(String[] args) throws ParseException {
        LogParser logParser = new LogParser(Paths.get("src/parser/logs"));
        System.out.println(logParser.getNumberOfUniqueIPs(null, new Date()));

        System.out.println(logParser.execute("get ip"));
        System.out.println(logParser.execute("get user"));
        System.out.println(logParser.execute("get ip for user = \"Eduard Petrovich Morozko\" and date between \"11.12.2013 0:00:00\" and \"03.01.2014 23:59:59\"."));
        System.out.println(logParser.execute("get event for status = \"OK\" and date between \"30.08.2012 16:08:40\" and \"05.01.2021 20:22:55\""));
    }
}