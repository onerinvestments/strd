package lmbrd.io;

import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: light
 * Date: 14/01/14
 * Time: 11:00
 */
public class JVMReservedMemory {
    public static AtomicInteger openedBuffers = new AtomicInteger(0);

    private static Field reservedMemory;

    private static Field maxMemory;

    static {

        try {

            Class clazz = Class.forName("java.nio.Bits");

            reservedMemory = clazz.getDeclaredField("reservedMemory");
            reservedMemory.setAccessible(true);

            maxMemory = clazz.getDeclaredField("maxMemory");
            maxMemory.setAccessible(true);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static long getReservedMemory() {
        return getValue(reservedMemory);
    }

    public static long getMaxMemory() {
        return getValue(maxMemory);
    }

    public static void setMaxMemory(long value) {
        try {
            maxMemory.setLong(null, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getValue(Field field) {

        try {

            return field.getLong(null);

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

    private static Field cleanerField;

    static {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16);
        try {
            cleanerField = byteBuffer.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

    }

    public static void clean(ByteBuffer bb) {
        Cleaner cleaner;
        try {
            cleaner = (Cleaner) cleanerField.get(bb);
            cleaner.clean();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static void cleanBuffer(ByteBuffer bb) {
        openedBuffers.decrementAndGet();
        clean(bb);
    }

    public static ByteBuffer buffer(int size) {
        openedBuffers.incrementAndGet();
        return ByteBuffer.allocateDirect(size);
    }

}