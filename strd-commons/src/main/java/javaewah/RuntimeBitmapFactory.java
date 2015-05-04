package javaewah;

import strd.bytes.InputBytes;

import java.io.IOException;
import java.util.BitSet;

/**
 * User: light
 * Date: 03/12/13
 * Time: 06:30
 */
public class RuntimeBitmapFactory {

    public static RuntimeBitmap newBitmap() {
        return wrapped(new BitSet());
    }

    public static RuntimeBitmap readEWAH(InputBytes bytes) {
        try {
            BitSet bitSet = new BitSet();

            EWAHCompressedBitmap bmp = new EWAHCompressedBitmap();
            bmp.deserialize(bytes);

            IntIterator iterator = bmp.intIterator();
            while (iterator.hasNext()) {
                bitSet.set(iterator.next());
            }

            return wrapped(bitSet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static RuntimeBitmap wrapped(BitSet set ) {
        return new WrappedBitSet(set);
    }


}
