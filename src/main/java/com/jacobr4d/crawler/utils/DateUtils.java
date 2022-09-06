package com.jacobr4d.crawler.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtils {
	
    static SimpleDateFormat sdf = null;
    static int initialized;
    
    public static SimpleDateFormat df() {
    	if (sdf != null) {
    		return sdf;
    	} else {
    		sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    		return sdf;
    	}
    }
    
    /* get time represented by http date string */
    public static Instant parseHttpDate(String httpDate) throws ParseException {
    	return df().parse(httpDate).toInstant();
    }
  
    
    /* format time as specified in assignment for /show route */
    public static String showDate(long epochSecond) {
    	Instant instant = Instant.ofEpochSecond(epochSecond);
    	SimpleDateFormat showDateFormat = new SimpleDateFormat("YYYY-MM-dd'T'hh:mm:ss");
    	showDateFormat.setTimeZone(TimeZone.getTimeZone("EST5EDT"));
    	return showDateFormat.format(Date.from(instant));
    }

}