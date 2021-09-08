package parser;


import parser.query.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {
    private final Path logDir;
    private final DateFormat dateFormat = new SimpleDateFormat("d.M.yyyy H:m:s");
    private final List<LogEntity> entities;


    public LogParser(Path logDir) {
        this.logDir = logDir;
        this.entities = new ArrayList<>();
        getAllEntities();
    }

    public boolean isBetweenDates (Date current, Date after, Date before) {
        if (after == null) {
            after = new Date(0);
        }
        if (before == null) {
            before = new Date(Long.MAX_VALUE);
        }
        return current.after(after) && current.before(before);
    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    private void getAllEntities() {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(logDir)) {
            for (Path file : directoryStream) {
                if (file.toString().toLowerCase().endsWith(".log")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                        while (reader.ready()) {
                            String[] params = reader.readLine().split("\\t");
                            Date date = dateFormat.parse(params[2]);
                            String[] taskAndTaskNum = params[3].split(" ");
                            entities.add(new LogEntity(params[0], params[1], date, Event.valueOf(taskAndTaskNum[0]), taskAndTaskNum.length > 1 ? Integer.parseInt(taskAndTaskNum[1]) : -1, Status.valueOf(params[4])));
                        }
                    } catch (IOException | ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                ips.add(entity.getIp());
            }
        }
        return ips;
    }


    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && isBetweenDates(entity.getDate(), after, before))  {
                ips.add(entity.getIp());
            }
        }
        return ips;
    }

    public Set<String> getIPsForDate(Date currentDate, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getDate().getTime() == currentDate.getTime() && isBetweenDates(entity.getDate(), after, before)) {
                ips.add(entity.getIp());
            }
        }
        return ips;
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(event) && isBetweenDates(entity.getDate(), after, before)) {
                ips.add(entity.getIp());
            }
        }
        return ips;
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEventStatus().equals(status) && isBetweenDates(entity.getDate(), after, before)) {
                ips.add(entity.getIp());
            }
        }
        return ips;
    }

    @Override
    public Set<String> getAllUsers() {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            users.add(entity.getName());
        }
        return users;
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users.size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && isBetweenDates(entity.getDate(), after, before)) {
                events.add(entity.getEvent());
            }
        }
        return events.size();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getIp().equals(ip) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.LOGIN) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    public Set<String> getUsersForStatus(Status status, Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEventStatus().equals(status)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.DOWNLOAD_PLUGIN) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.WRITE_MESSAGE) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.SOLVE_TASK) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.SOLVE_TASK) && entity.getEventNum() == task && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.DONE_TASK) && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(Event.DONE_TASK) && entity.getEventNum() == task && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                if ((user == null || entity.getName().equals(user)) && (event == null || entity.getEvent().equals(event))) {
                    dates.add(entity.getDate());
                }
            }
        }
        return dates;
    }

    public Set<Date> getDatesForStatus(Status status, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEventStatus().equals(status)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEventStatus().equals(Status.FAILED) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEventStatus().equals(Status.ERROR) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && entity.getEvent().equals(Event.LOGIN) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        if (dates.size() == 0) {
            return null;
        }
        return dates.stream().min(Comparator.comparingLong(Date::getTime)).get();
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && entity.getEvent().equals(Event.SOLVE_TASK) && entity.getEventNum() == task && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        if (dates.size() == 0) {
            return null;
        }
        return dates.stream().min(Comparator.comparingLong(Date::getTime)).get();
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && entity.getEvent().equals(Event.DONE_TASK) && entity.getEventNum() == task && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        if (dates.size() == 0) {
            return null;
        }
        return dates.stream().min(Comparator.comparingLong(Date::getTime)).get();
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && entity.getEvent().equals(Event.WRITE_MESSAGE) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && entity.getEvent().equals(Event.DOWNLOAD_PLUGIN) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }
    
    public Set<Date> getAllDates(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }

    public Set<Date> getDatesForIPs(String ip, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (LogEntity entity : entities) {
            if (ip.equals(entity.getIp()) && isBetweenDates(entity.getDate(), after, before)) {
                dates.add(entity.getDate());
            }
        }
        return dates;
    }


    public Set<String> getUsersForDate(Date currentDate, Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getDate().getTime() == currentDate.getTime() && isBetweenDates(entity.getDate(), after, before)) {
                users.add(entity.getName());
            }
        }
        return users;
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        return getAllEvents(after, before).size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getIp().equals(ip)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getName().equals(user)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEventStatus().equals(Status.FAILED)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEventStatus().equals(Status.ERROR)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    public Set<Event> getEventsForDate(Date current, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getDate().getTime() == current.getTime() && isBetweenDates(entity.getDate(), after, before)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    public Set<Event> getEventsForStatus(Status status, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEventStatus().equals(status) && isBetweenDates(entity.getDate(), after, before)) {
                events.add(entity.getEvent());
            }
        }
        return events;
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        int result = 0;
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEvent().equals(Event.SOLVE_TASK) && entity.getEventNum() == task) {
                result++;
            }
        }
        return result;
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        int result = 0;
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEvent().equals(Event.DONE_TASK) && entity.getEventNum() == task) {
                result++;
            }
        }
        return result;
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> tasks = new HashMap<>();
        int eventNum;
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEvent().equals(Event.SOLVE_TASK)) {
                eventNum = entity.getEventNum();
                if (tasks.containsKey(eventNum)) {
                    tasks.put(eventNum, tasks.get(eventNum) + 1);
                } else {
                    tasks.put(eventNum, 1);
                }
            }
        }
        return tasks;
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> tasks = new HashMap<>();
        int eventNum;
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before) && entity.getEvent().equals(Event.DONE_TASK)) {
                eventNum = entity.getEventNum();
                if (tasks.containsKey(eventNum)) {
                    tasks.put(eventNum, tasks.get(eventNum) + 1);
                } else {
                    tasks.put(eventNum, 1);
                }
            }
        }
        return tasks;
    }
    
    public Set<Status> getAllStatuses(Date after, Date before) {
        Set<Status> statuses = new HashSet<>();
        for (LogEntity entity : entities) {
            if (isBetweenDates(entity.getDate(), after, before)) {
                statuses.add(entity.getEventStatus());
            }
        }
        return statuses;
    }



    public Set<Status> getStatusesForIp(String ip, Date after, Date before) {
        Set<Status> statuses = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getIp().equals(ip) && isBetweenDates(entity.getDate(), after, before)) {
                statuses.add(entity.getEventStatus());
            }
        }
        return statuses;
    }

    public Set<Status> getStatusesForUser(String user, Date after, Date before) {
        Set<Status> statuses = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getName().equals(user) && isBetweenDates(entity.getDate(), after, before)) {
                statuses.add(entity.getEventStatus());
            }
        }
        return statuses;
    }

    public Set<Status> getStatusesForDate(Date currentDate, Date after, Date before) {
        Set<Status> statuses = new HashSet<>();
        for (LogEntity entity: entities) {
            if (entity.getDate().getTime() == currentDate.getTime() && isBetweenDates(entity.getDate(), after, before)) {
                statuses.add(entity.getEventStatus());
            }
        }
        return statuses;
    }

    public Set<Status> getStatusesForEvent(Event event, Date after, Date before) {
        Set<Status> statuses = new HashSet<>();
        for (LogEntity entity : entities) {
            if (entity.getEvent().equals(event) && isBetweenDates(entity.getDate(), after, before)) {
                statuses.add(entity.getEventStatus());
            }
        }
        return statuses;
    }

    @Override
    public Set<Object> execute(String query) throws ParseException {
        Set result = null;
        String param = null;
        Date after = null;
        Date before = null;
        if (query.contains("=")) {
            int firstQuote = query.indexOf("\"");
            int secondQuote = query.indexOf("\"", firstQuote + 1);
            param = query.substring(firstQuote + 1, secondQuote);
        }
        if (query.contains("date between")) {
            String[] strings = query.split("date between");
            strings = strings[1].split("\"");
            after = dateFormat.parse(strings[1]);
            before = dateFormat.parse(strings[3]);
        }
        if (query.startsWith("get ip")) {
            if (!query.contains("for")) {
                result = getUniqueIPs(after, before);
            } else if (query.contains("user")) {
                result = getIPsForUser(param, after, before);
            } else if (query.contains("for date")) {
                result = getIPsForDate(dateFormat.parse(param), after, before);
            } else if (query.contains("event")) {
                result = getIPsForEvent(Event.valueOf(param), after, before);
            } else if (query.contains("status")) {
                result = getIPsForStatus(Status.valueOf(param), after, before);
            }
        } else if (query.startsWith("get user")) {
            if (!query.contains("for")) {
                result = getAllUsers();
            } else if (query.contains("ip")) {
                result = getUsersForIP(param, after, before);
            } else if (query.contains("for date")) {
                result = getUsersForDate(dateFormat.parse(param), after, before);
            } else if (query.contains("event")) {
                Event event = Event.valueOf(param);
                if (Event.DONE_TASK.equals(event)) {
                    result = getDoneTaskUsers(after, before);
                }
                if (Event.LOGIN.equals(event)) {
                    result = getLoggedUsers(after, before);
                }
                if (Event.DOWNLOAD_PLUGIN.equals(event)) {
                    result = getDownloadedPluginUsers(after, before);
                }
                if (Event.SOLVE_TASK.equals(event)) {
                    result = getSolvedTaskUsers(after, before);
                }
                if (Event.WRITE_MESSAGE.equals(event)) {
                    result = getWroteMessageUsers(after, before);
                }
            } else if (query.contains("status")) {
                result = getUsersForStatus(Status.valueOf(param), after, before);
            }
        } else if (query.startsWith("get date")) {
            if (!query.contains("for")) {
                result = getAllDates(after, before);
            } else if (query.contains("ip")) {
                result = getDatesForIPs(param, after, before);
            } else if (query.contains("user")) {
                result = getDatesForUserAndEvent(param, null, after, before);
            } else if (query.contains("event")) {
                result = getDatesForUserAndEvent(null, Event.valueOf(param), after, before);
            } else if (query.contains("status")) {
                result = getDatesForStatus(Status.valueOf(param), after, before);
            }
        } else if (query.startsWith("get event")) {
            if (!query.contains("for")) {
                result = getAllEvents(after, before);
            } else if (query.contains("ip")) {
                result = getEventsForIP(param, after, before);
            } else if (query.contains("user")) {
                result = getEventsForUser(param, after, before);
            } else if (query.contains("for date")) {
                result = getEventsForDate(dateFormat.parse(param), after, before);
            } else if (query.contains("status")) {
                result = getEventsForStatus(Status.valueOf(param), after, before);
            }
        } else if (query.startsWith("get status")) {
            if (!query.contains("for")) {
                result = getAllStatuses(after, before);
            } else if (query.contains("ip")) {
                result = getStatusesForIp(param, after, before);
            } else if (query.contains("user")) {
                result = getStatusesForUser(param, after, before);
            } else if (query.contains("for date")) {
                result = getStatusesForDate(dateFormat.parse(param), after, before);
            } else if (query.contains("event")) {
                result = getStatusesForEvent(Event.valueOf(param), after, before);
            }
        }
        return result;
    }

    public class LogEntity {
        private final String ip;
        private final String name;
        private final Date date;
        private final Event event;
        private final int eventNum;
        private final Status eventStatus;

        public LogEntity(String ip, String name, Date date, Event event, int eventNum, Status eventStatus) {
            this.ip = ip;
            this.name = name;
            this.date = date;
            this.event = event;
            this.eventNum = eventNum;
            this.eventStatus = eventStatus;
        }

        public String getIp() {
            return ip;
        }

        public String getName() {
            return name;
        }

        public Date getDate() {
            return date;
        }

        public Event getEvent() {
            return event;
        }

        public int getEventNum() {
            return eventNum;
        }

        public Status getEventStatus() {
            return eventStatus;
        }
    }
}