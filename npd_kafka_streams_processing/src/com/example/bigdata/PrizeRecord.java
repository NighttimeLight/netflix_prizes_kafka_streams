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

public class PrizeRecord implements Serializable {

    private static final Logger logger = Logger.getLogger("Prize_Data");
    private static final String PRIZE_LINE_PATTERN = "^([\\d-]{10}),(\\d+),(\\d+),(\\d+)$";
    private static final Pattern PATTERN = Pattern.compile(PRIZE_LINE_PATTERN);
    private String dateTimeString;
    private int filmId;
    private int userId;
    private int rate;

    public PrizeRecord(String dateString, String filmIdString, String userIdString, String rateString) {
        this.setDateTimeString(dateString);
        this.setFilmId(Integer.parseInt(filmIdString));
        this.setUserId(Integer.parseInt(userIdString));
        this.setRate(Integer.parseInt(rateString));
    }

    public static PrizeRecord parseFromStringLine(String stringLine) {
        Matcher m = PATTERN.matcher(stringLine);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse stringLine" + stringLine);
            throw new RuntimeException("Error parsing stringLine: " + stringLine);
        }
        return new PrizeRecord(m.group(1), m.group(2), m.group(3), m.group(4));
    }

    public static boolean lineIsCorrect(String stringLine) {
        Matcher m = PATTERN.matcher(stringLine);
        return m.find();
    }

    public long getTimestampInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd", Locale.US);
        Date date;
        try {
            date = sdf.parse(getDateTimeString());
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s", getDateTimeString(), getFilmId(), getUserId(), getRate());
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public int getFilmId() {
        return filmId;
    }

    public void setFilmId(int filmId) {
        this.filmId = filmId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }
}
