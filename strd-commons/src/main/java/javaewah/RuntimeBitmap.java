package javaewah;

/**
 * User: light
 * Date: 03/12/13
 * Time: 06:25
 */
public interface RuntimeBitmap {
    void strictSet(final int i);

    RuntimeBitmap and(RuntimeBitmap another);

    RuntimeBitmap or(RuntimeBitmap another);

    int cardinality();

    boolean isEmpty();

    boolean isSet(int index );

    IntIterator intIterator();

    public static final RuntimeBitmap EMPTY_BITMAP =
            RuntimeBitmapFactory.newBitmap();

    RuntimeBitmap copy();


}
