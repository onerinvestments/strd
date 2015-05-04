package strd;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/26/13
 * Time: 3:48 PM
 */
public class IpUtils {

    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final Pattern addressPattern = Pattern.compile(IP_ADDRESS);

    /*
         * Convert a dotted decimal format address to a packed integer format
         */
    public static int ipToInt(String address) {
        Matcher matcher = addressPattern.matcher(address);
        if (matcher.matches()) {
            return matchAddress(matcher);
        } else
            throw new IllegalArgumentException("Could not parse [" + address + "]");
    }

    /*
         * Convenience method to extract the components of a dotted decimal address and
         * pack into an integer using a regex match
         */
    private static int matchAddress(Matcher matcher) {
        int addr = 0;
        for (int i = 1; i <= 4; ++i) {
            int n = (rangeCheck(Integer.parseInt(matcher.group(i)), -1, 255));
            addr |= ((n & 0xff) << 8 * (4 - i));
        }
        return addr;
    }

    /**
     * Convenience function to check integer boundaries.
     * Checks if a value x is in the range (begin,end].
     * Returns x if it is in range, throws an exception otherwise.
     */
    public static int rangeCheck(int value, int begin, int end) {
        if (value > begin && value <= end) // (begin,end]
            return value;

        throw new IllegalArgumentException("Value [" + value + "] not in range (" + begin + "," + end + "]");
    }

    public static String intToIp(int ip) {
        return ((ip >> 24) & 0xFF) + "." +

                ((ip >> 16) & 0xFF) + "." +

                ((ip >> 8) & 0xFF) + "." +

                (ip & 0xFF);

    }

    public static void main(String[] args) {
        System.out.println(intToIp(ipToInt("127.0.0.1")));
        System.out.println(intToIp(ipToInt("255.255.255.255")));
        System.out.println(intToIp(ipToInt("10.20.30.40")));
    }
}
