package strd;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.google.common.cache.*;
import org.slf4j.Marker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: penkov
 * Date: 20.08.14
 * Time: 14:46
 */
public class DuplicateFilter extends TurboFilter {

    public static final int MAX_KEY_TOKEN_LENGTH = 30;
    private long period = 1000;

    private LoadingCache<String, EntryData> cache;
    private static final Lock lock = new ReentrantLock(  );

    private int maxRepeats = 5;


    @Override
    public void start() {
        super.start();

        cache = CacheBuilder.newBuilder()
                .maximumSize( 1000 )
                .expireAfterWrite( 1100, TimeUnit.MILLISECONDS )
                .removalListener( new RemovalListener<String, EntryData>() {
                    @Override
                    public void onRemoval( RemovalNotification<String, EntryData> notification ) {
                        if ( notification.getCause() == RemovalCause.EXPIRED ) {
                            traceRepeats( notification.getKey(), notification.getValue().count.get() - maxRepeats );
                        }
                    }
                } )
                .build(
                        new CacheLoader<String, EntryData>() {
                            public EntryData load( String key ) throws Exception {
                                return new EntryData( System.currentTimeMillis() );
                            }
                        } );

    }

    @Override
    public void stop() {
        super.stop();
        cache.cleanUp();
    }

    @Override
    public FilterReply decide( Marker marker, Logger logger, Level level, String format, Object[] params, Throwable t ) {

        long now = System.currentTimeMillis();
        FilterReply res = FilterReply.NEUTRAL;

        String key = formatKey( format, t );

        if (level.levelInt < Level.DEBUG.levelInt ||  key == null || key.length() <= 0) {
            return res;
        }


        EntryData entry;
        try {
            entry = cache.get( key );
        } catch ( ExecutionException e ) {
            throw new RuntimeException( e );
        }


        if ( now - entry.timeAdded <= period ) {
            int repeats = entry.count.incrementAndGet();
            if ( repeats > maxRepeats ) {
                res = FilterReply.DENY;
            }
        }
        else {
            lock.lock();
            try {
                try {
                    entry = cache.get( key );
                } catch ( ExecutionException e ) {
                    throw new RuntimeException( e );
                }

                if ( now - entry.timeAdded <= period ) {
                    int repeats = entry.count.incrementAndGet();
                    if ( repeats > maxRepeats ) {
                        res = FilterReply.DENY;
                    }
                }
                else {
                    int repeats = entry.count.get();
                    cache.put( key, new EntryData( now ) );
                    traceRepeats( key, repeats - maxRepeats );
                }
            }
            finally {
                lock.unlock();
            }
        }

        return res;
    }

    public String formatKey( String format, Throwable t ) {
        StringBuilder sb = new StringBuilder();
        if ( format != null ) {
            if ( format.length() > MAX_KEY_TOKEN_LENGTH ) {
                sb.append( format.substring( 0, MAX_KEY_TOKEN_LENGTH ) ).append( "..." );
            } else {
                sb.append( format );
            }
        }
        if ( t != null && t.getMessage() != null && t.getMessage().length() > 0) {

            sb.append( " | " ).append(t.getClass().getName()).append( ": " );
            String excepMsg = t.getMessage();
            if ( excepMsg.length() > MAX_KEY_TOKEN_LENGTH ) {
                sb.append( excepMsg.substring( 0, MAX_KEY_TOKEN_LENGTH ) ).append( "..." );
            } else {
                sb.append( excepMsg );
            }
        }

        return sb.toString();
    }

    public void traceRepeats(String message, int repeats ) {
        if ( repeats > maxRepeats ) {
            System.out.println( "Message '" + message + "' repeated " + repeats + " times" );
        }
    }


    private class EntryData {

        public final long timeAdded;
        public AtomicInteger count = new AtomicInteger( 0 );

        private EntryData( long timeAdded ) {
            this.timeAdded = timeAdded;
        }
    }


    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public int getMaxRepeats() {
        return maxRepeats;
    }

    public void setMaxRepeats(int maxRepeats) {
        this.maxRepeats = maxRepeats;
    }
}
