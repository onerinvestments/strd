package lmbrd.zn.util;

import io.netty.handler.codec.http.DefaultCookie;

/**
 * User: light
 * Date: 9/16/13
 * Time: 12:06 PM
 */
public class CookieWithAge extends DefaultCookie{

    public static int AGE= (int) (365L * 10 * TimeUtil.aDAY / 1000L);

    /**
     * Creates a new cookie with the specified name and value.
     */
    public CookieWithAge(String domain, String name, String value) {
        super(name, value);
        setMaxAge(AGE);
        setPath("/");
        setDomain(domain);
    }
}
