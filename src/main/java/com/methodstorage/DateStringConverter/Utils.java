package main.java.com.methodstorage.DateStringConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Utils {

    /*
    PRACTICES:
        1. Do not re-set a method's parameter, instead of that define and use a new variable
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());

    //Multi-thread supported
    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final static DateTimeFormatter dateToStringFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /*Multi-thread NOT supported (F.e. if a multi-thread running Spark job uses this method, we will
     be getting some errored/inaccurate results due to SimpleDateFormat not supporting multi-thread)*/
    private final static SimpleDateFormat dateFormatter2 = new SimpleDateFormat("dd/MM/yyyy");
    private final static SimpleDateFormat dateToStringFormatter2 = new SimpleDateFormat("yyyy-MM-dd");


    //METHOD#1:
    //To manually convert timestamp field in seconds to timestamp in milliseconds (epoch time)
    public static Long convertSecondToMs(Long timestamp) {
        return timestamp != null ? timestamp * 1000 : null;
    }

    //METHOD#2:
    //To automatically convert timestamp field in seconds to timestamp in milliseconds (epoch time)
    public static Long processTimestamp(Long timestamp) {
        //1000000000000L (epoch) = "2001-09-09"
        final long minTimestamp = 1000000000000L;
        if (timestamp != null) {
            if (timestamp < minTimestamp) {
                return timestamp * 1000;
            }
        }
        return timestamp;
    }

    //METHOD#2.1:
    /*An ineffective way to automatically convert timestamp field in seconds to timestamp in
     milliseconds (epoch time) due to unnecessary and slow String casting*/
    public static Long processTimestamp2(Long timestamp) {
        Long result = timestamp;
        if (result != null) {
            int tsLength = (Long.toString(timestamp)).length(); /*Converting Long field to String then
             checking its character length to determine if the format is in seconds of milliseconds*/
            if (tsLength == 10) {
                return result * 1000;
            } else if (tsLength != 13) {
                LOGGER.error("Error: Unexpected timestamp format: " + timestamp);
            }
        }
        return result;
    }

    //METHOD#3:
    //Multi-thread supported

    /*Parsing f.e. String "01/05/1994" to LocalDate "1994-05-01", then re-formatting as String
    "1994-05-01". This way, we will be able to correctly range the data on a SQL query with a
    String field named 'date'*/
    public static String dateConverter(String date) {
        String result = date;
        if (result != null) {
            try {
                LocalDate dateFormat = LocalDate.parse(result, dateFormatter);
                result = dateFormat.format(dateToStringFormatter);
            } catch (Exception e) {
                LOGGER.error("Error: An exception occurred while parsing: " + date, e);
            }
        }
        return result;
    }

    //METHOD#3.1:
    /*Multi-thread NOT supported (F.e. if a multi-thread running Spark job uses this method, we will
     be getting some errored/inaccurate results due to SimpleDateFormat not supporting multi-thread)*/

    /*Almost same as Method#3: Parsing f.e. String "09/01/1998" to Date "Fri Jan 09 00:00:00 TRT 1998",
     then re-formatting as String "1998-01-09". This way, we will be able to correctly range the
     data on a SQL query with a String field named 'date'*/
    public static String dateConverter2(String date) {
        String result = date;
        if (result != null) {
            try {
                Date dateFormat = dateFormatter2.parse(result);
                result = dateToStringFormatter2.format(dateFormat);
            } catch (ParseException e) {
                LOGGER.error("Error: An exception occurred while parsing: " + date, e);
            }
        }
        return result;
    }


}
