package com.example.bigdata;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieRecord implements Serializable {

    private static final Logger logger = Logger.getLogger("Movies");
    private static final String MOVIE_LINE_PATTERN = "^(\\d+),([\\da-zA-Z]+),(.+)$";
    private static final Pattern PATTERN = Pattern.compile(MOVIE_LINE_PATTERN);
    private int id;
    private String year;
    private String title;

    public MovieRecord(String idStr, String yr, String tit) {
        this.setId(Integer.parseInt(idStr));
        this.setYear(yr);
        this.setTitle(tit);
    }

    public static MovieRecord parseFromStringLine(String stringLine) {
        Matcher m = PATTERN.matcher(stringLine);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse stringLine" + stringLine);
            throw new RuntimeException("Error parsing stringLine: " + stringLine);
        }
        return new MovieRecord(m.group(1), m.group(2), m.group(3));
    }

    public static boolean lineIsCorrect(String stringLine) {
        Matcher m = PATTERN.matcher(stringLine);
        return m.find();
    }

    @Override
    public String toString() {
        return String.format("%s", getTitle());
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
