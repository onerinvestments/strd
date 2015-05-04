package lmbrd.zn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * User: light
 * Date: 19/08/14
 * Time: 13:09
 */
public class TimeSuppressor {

	private static final TimeSuppressor instance = new TimeSuppressor();

	private final ConcurrentHashMap<Object, Long> times =
			new ConcurrentHashMap<Object, Long>();


	public static TimeSuppressor getInstance() {
		return instance;
	}

	public static boolean can(Object id, long interval) {
		return getInstance().canDo( id, interval );
	}

	public static boolean can(Class cl, String id, long interval) {
		return getInstance().canDo( cl.getName() + "_" + id, interval );
	}

	public boolean canDo(Object id, long interval) {
		return canDo( id, interval, System.currentTimeMillis() );
	}

    public Long getTimeEntry(Object key) {
        Long value = times.get(key);
        if (value == null) {
            synchronized (times) {
                value = times.get(key);
                if (value == null) {
                    value = System.currentTimeMillis();
                    times.put(key, value);
                }
            }
        }
        return value;
    }

	public boolean canDo(Object id, long interval, long now) {
		if ( times.size() > 100000 ) {
			final Logger log =
					LoggerFactory.getLogger(getClass());
			log.error( "TimesPool is very large -> possible leak" );
			return true;
		}

		Long lastTime = getTimeEntry(id);

		if ( now - lastTime < 0 ) {
			return true;
		} else if ( now - lastTime >= interval ) {
			times.put( id, now );
			return true;
		} else {
			return false;
		}
	}


}
