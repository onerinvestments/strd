package strd.storage;

import javaewah.IntIterator;
import javaewah.RuntimeBitmap;

/**
 * User: light
 * Date: 9/2/13
 * Time: 3:53 PM
 */
public class RecordFieldSource {
    public final IntIterator iter;
    public final RuntimeBitmap bmp;

    public final Object value;

    public final int fieldNum;

    public RecordFieldSource(int fieldNum,  Object value, RuntimeBitmap bmp ) {
        this.fieldNum = fieldNum;
        this.bmp = bmp;
        this.value = value;
        this.iter = bmp.intIterator();
        iter.hasNext();
    }

}
