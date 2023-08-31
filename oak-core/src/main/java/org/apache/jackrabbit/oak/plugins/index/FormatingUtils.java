package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class FormatingUtils {
    public static String formatToSeconds(Stopwatch stopwatch) {
        LocalTime seconds = LocalTime.ofSecondOfDay(stopwatch.elapsed(TimeUnit.SECONDS));
        return DateTimeFormatter.ISO_TIME.format(seconds);
    }

    public static String formatToMillis(Stopwatch stopwatch) {
        LocalTime nanoSeconds = LocalTime.ofNanoOfDay(stopwatch.elapsed(TimeUnit.MILLISECONDS)*1000000);
        return DateTimeFormatter.ISO_TIME.format(nanoSeconds);
    }
}
