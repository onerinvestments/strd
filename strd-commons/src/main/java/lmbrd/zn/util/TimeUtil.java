package lmbrd.zn.util;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * $Id$
 * $URL$
 * User: light
 * Date: 8/4/11
 * Time: 8:20 PM
 */
public class TimeUtil {

    public static final long aSECOND = 1000;
    public static final long aMINUTE = aSECOND * 60;
    public static final long aHOUR = aMINUTE * 60;
    public static final long aDAY = aHOUR * 24;

    private static TimeZone timeZone = Calendar.getInstance().getTimeZone();

    private static final long[] monthes;

    private static final long startMonthTime;

    static {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

            long from = dateFormat.parse("2010-01-01").getTime();
            long to = dateFormat.parse("2060-01-01").getTime();

            startMonthTime = from;

            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(from);

            long time = from;

            ArrayList<Long> lst = new ArrayList<>();
            while (time < to) {

                lst.add(time);

                cal.add(Calendar.MONTH, 1);

                time = cal.getTimeInMillis();
            }

            monthes = new long[lst.size()];

            for (int i = 0; i < lst.size(); i++) {
                Long aLong = lst.get(i);
                monthes[i] = aLong;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static long clearMinute(long timeInMillis) {
        return (timeInMillis / aMINUTE) * aMINUTE;
    }

    public static long clearHour(long timeInMillis) {
        return (timeInMillis / aHOUR) * aHOUR;
    }

    public static long clearDayGMT(long timeInMillis) {
        return (timeInMillis / aDAY) * aDAY;
    }

    public static long clearDay(long timeInMillis) {
/*
        final int offset = timeZone.getOffset( timeInMillis );
		timeInMillis += offset;
		return (timeInMillis / aDAY) * aDAY;
*/
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);

        return cal.getTimeInMillis();

    }

    public static long clearDayAndAddDays(Date date, int delta) {
        date.setTime(TimeUtil.clearDay(date.getTime()));
        Calendar tempCalendar = Calendar.getInstance();
        tempCalendar.setTime(date);
        tempCalendar.add(Calendar.DAY_OF_YEAR, delta);
        date.setTime(tempCalendar.getTimeInMillis());
        return date.getTime();
    }

    public static long endDay(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        cal.set(Calendar.MILLISECOND, 999);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.HOUR_OF_DAY, 23);

        return cal.getTimeInMillis();
    }

    public static TimeGroupType suggestGrouping( Date from, Date to ) {
        Calendar start = Calendar.getInstance();
        start.setTime( from );

        Calendar end = Calendar.getInstance();
        end.setTime( to );


        long endL = end.getTimeInMillis() + end.getTimeZone().getOffset( end.getTimeInMillis() );
        long startL = start.getTimeInMillis() + start.getTimeZone().getOffset( start.getTimeInMillis() );
        long dayCount = (endL - startL) / aDAY;

        if ( dayCount >= 1 && dayCount < 60 ) {
            return TimeGroupType.DAY;
        } else if ( dayCount >= 60 ) {
            return TimeGroupType.MONTH;
        } else {
            return TimeGroupType.HOUR;
        }

    }

    public static void main(String[] args) {
//		long time = System.currentTimeMillis();

        CachedTimeTransformer allCacherMonth = new CachedTimeTransformer(TimeGroupType.MONTH);
        CachedTimeTransformer allCacherDay = new CachedTimeTransformer(TimeGroupType.DAY);

        for (int i = 0; i < 900; i++) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis());
            cal.add(Calendar.SECOND, +i);
            cal.add(Calendar.HOUR_OF_DAY, +i);
            cal.add(Calendar.DAY_OF_MONTH, +i);

            long value = cal.getTimeInMillis();

            CachedTimeTransformer newCacherMonth = new CachedTimeTransformer(TimeGroupType.MONTH);
            CachedTimeTransformer newCacherDay = new CachedTimeTransformer(TimeGroupType.DAY);


            if (clearMonth(value) != clearMonthOld(value)) {
                throw new RuntimeException("Bad month:" + cal.getTime() + "    " + clearMonthOld(value) + "   !=    " + clearMonth(value));
            }

            if (allCacherDay.clearTime(value) != clearDay(value)) {
                throw new RuntimeException();
            }

            if (newCacherDay.clearTime(value) != clearDay(value)) {
                throw new RuntimeException();
            }


            if (allCacherMonth.clearTime(value) != clearMonthOld(value)) {
                throw new RuntimeException();
            }

            if (newCacherMonth.clearTime(value) != clearMonthOld(value)) {
                throw new RuntimeException();
            }

            //check( cal.getTimeInMillis() );
        }
    }

    @SuppressWarnings("deprecation")
    private static void check(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);

        final long v1 = cal.getTimeInMillis();
        final long v2 = clearDay(time);


        if (v1 != v2) {
            System.out.println("dt0: " + new Date(time));
            System.out.println("dt1: " + new Date(v1));
            System.out.println("dt2: " + new Date(v2));
            System.out.println("dt3: " + new Date(v2).toGMTString());

            System.out.println("v2: " + v2 + " time: " + time);

            throw new RuntimeException("diff " + v1 + " != " + v2);
        }
    }


    public static long clearMonth(long hour) {
        int idx = Arrays.binarySearch(monthes, hour);
        if (idx > 0) {
            return hour;
        } else {
            return monthes[(-idx) - 2];
        }

    }

    public static long clearWeek(long timeInMillis) {
    /*
            final int offset = timeZone.getOffset( timeInMillis );
    		timeInMillis += offset;
    		return (timeInMillis / aDAY) * aDAY;
    */
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

        return cal.getTimeInMillis();

    }

    public static long clearMonthOld(long hour) {

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(hour);

        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);

        return c.getTimeInMillis();
    }

    public static class Time {

        public final long time;
        public final long minute;
        public final long hour;
        public final long day;
        private long month;
        private long week;

        public Time(long time) {
            this.time = time;

            this.minute = clearMinute(time);
            this.hour = clearHour(time);
            this.day = clearDay(time);
        }

        public long getMonth() {
            if (this.month == 0) {
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(this.hour);
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.DAY_OF_MONTH, 1);
                this.month = c.getTimeInMillis();
            }

            return this.month;
        }

        public long getWeek() {
            if (this.week == 0) {
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(this.hour);
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
                this.week = c.getTimeInMillis();
            }

            return this.week;
        }

        public long getForTimeGroup(TimeGroupType group) {
            switch (group) {

                case MINUTE:
                    return minute;
                case HOUR:
                    return hour;
                case DAY:
                    return day;
                case MONTH:
                    return getMonth();
                case WEEK:
                    return getWeek();
                case ALL:
                    return 0;
            }

            throw new IllegalArgumentException("Bad type group: " + group);
        }
    }

    @XmlEnum
    public static enum TimeGroupType {
        @XmlEnumValue(value = "min")
        MINUTE(1),
        @XmlEnumValue(value = "h")
        HOUR(2),
        @XmlEnumValue(value = "day")
        DAY(3),
        @XmlEnumValue(value = "month")
        MONTH(4),
        @XmlEnumValue(value = "all")
        ALL(5),
        @XmlEnumValue(value = "week")
        WEEK(6),
        @XmlEnumValue(value = "none")
        NONE(7)
        //
        ;

        private int id;

        private byte[] byteId;

        TimeGroupType(int id) {
            this.id = id;
            this.byteId = new byte[]{(byte) id};
        }

        public static TimeGroupType forId(int id) {
            switch (id) {
                case 1:
                    return MINUTE;
                case 2:
                    return HOUR;
                case 3:
                    return DAY;
                case 4:
                    return MONTH;
                case 5:
                    return ALL;
                case 6:
                    return WEEK;
                case 7:
                    return NONE;
                default:
                    return null;
            }
        }

        public byte[] getByteId() {
            return byteId;
        }

        public int getId() {
            return id;
        }
    }


    public static class CachedTimeTransformer {
        public final TimeGroupType group;

        private long[] nearest;

        private static final int BUF = 24;
        private int fieldId;

        public CachedTimeTransformer(TimeGroupType group) {
            this.group = group;

            switch (group) {
                case MINUTE:
                    fieldId = Calendar.MINUTE;
                    break;
                case HOUR:
                    fieldId = Calendar.HOUR_OF_DAY;
                    break;
                case DAY:
                    fieldId = Calendar.DAY_OF_MONTH;
                    break;
                case WEEK:
                    fieldId = Calendar.WEEK_OF_YEAR;
                    break;
                case MONTH:
                    fieldId = Calendar.MONTH;
                    break;
                case ALL:
                    fieldId = Calendar.ERA;
                    break;
                case NONE:
                    fieldId = Calendar.ERA;
                    break;

                default:
                    throw new RuntimeException();
            }
        }

        public long clearTime(long value) {
            if (group == TimeUtil.TimeGroupType.HOUR) {
                return TimeUtil.clearHour(value);

            } else if (group == TimeUtil.TimeGroupType.MINUTE) {
                return TimeUtil.clearMinute(value);

            } else if (group == TimeGroupType.ALL) {
                return 0;
            } else if (group == TimeGroupType.NONE) {
                return value;
            } else {
                if (nearest == null) {
                    populateNearest(value);
                }

                int idx = Arrays.binarySearch(nearest, value);
                if (idx < 0) {
                    if (idx < -BUF || idx == -1) {
                        return truncateValueImpl(value);
                    } else {
                        idx = (-idx) - 2;
                        return nearest[idx];
                    }
                } else {
                    return value;
                }
            }
        }


        private void populateNearest(long value) {
            nearest = new long[BUF];

            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(truncateValueImpl(value));
            cal.add(fieldId, -BUF / 2);

            for (int i = 0; i < BUF; i++) {
                cal.setTimeInMillis(truncateValueImpl(value));
                cal.add(fieldId, -(BUF / 2) + i);
                nearest[i] = cal.getTimeInMillis();
            }

        }

        private long truncateValueImpl(long value) {
            switch (group) {
                case MINUTE:
                    return TimeUtil.clearMinute(value);

                case HOUR:
                    return TimeUtil.clearHour(value);

                case DAY:
                    return TimeUtil.clearDay(value);

                case MONTH:
                    return TimeUtil.clearMonth(value);

                case WEEK:
                    return TimeUtil.clearWeek(value);

                case NONE:
                    return value;

                case ALL:
                    return 0;

                default:
                    throw new IllegalArgumentException("bad group:" + group);
            }
        }


    }
}
