package strd.storage;

import strd.node.impl.databuffers.DataBuffer;
import strd.node.storage.ObjectListBuffer;

/**
 * User: light
 * Date: 03/12/13
 * Time: 16:55
 *
 * Iterator on ObjectListBuffer
 */
public class RecordFieldSource2 {

    public int currentIndex;
    private Object currentValue;

    public final int selectSourceId;

    public RecordFieldSource2(int fieldNum, ObjectListBuffer buffer) {
        this.selectSourceId = fieldNum;
        this.buffer = buffer.values;
        this.currentIndex = -1;
        this.length = this.buffer.length;

        next();
    }


    public static RecordFieldSource2 createSource(int selectSourceId, DataBuffer value) {
        if (value instanceof ObjectListBuffer) {
            return new RecordFieldSource2(selectSourceId, (ObjectListBuffer) value);
        } else {
            throw new RuntimeException();
        }
    }

    private final Object[] buffer;
    private int length;


    public Object getValue() {
        if (currentValue == null) {
            throw new IllegalStateException("index = " + currentIndex + "/" +length);
        }
        return currentValue;
    }

    public int next() {
        this.currentValue = null;

        for (int i = currentIndex + 1; i < length; i++) {
            Object o = buffer[i];

            if ( o != null ) {
                this.currentIndex = i;
                this.currentValue = o;
                return this.currentIndex;
            }

        }

        currentIndex = -1;
        return -1;
    }

}
