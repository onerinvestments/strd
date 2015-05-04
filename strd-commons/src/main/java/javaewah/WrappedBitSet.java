package javaewah;

import java.util.BitSet;

/**
 * User: light
 * Date: 03/12/13
 * Time: 06:42
 */
public class WrappedBitSet implements RuntimeBitmap {
    private final BitSet set;

    public WrappedBitSet(BitSet set) {
        this.set = set;
    }

    @Override
    public void strictSet(int i) {
        set.set(i);
    }

    @Override
    public RuntimeBitmap and(RuntimeBitmap another) {
        set.and(((WrappedBitSet) another).set);
        return this;
    }

    @Override
    public RuntimeBitmap or(RuntimeBitmap another) {
        set.or(((WrappedBitSet) another).set);
        return this;
    }

    @Override
    public int cardinality() {
        return set.cardinality();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean isSet(int index) {
        return set.get(index);
    }

    @Override
    public IntIterator intIterator() {

        return new IntIterator() {
            int index = 0;

            @Override
            public int next() {
                int i = index;

                if (i == -1) {
                    throw new IllegalStateException();
                }
                index ++;
                return i;
            }

            @Override
            public boolean hasNext() {
                index = set.nextSetBit(index);
                return index != -1;
            }
        };
    }

    @Override
    public RuntimeBitmap copy() {
        return new WrappedBitSet((BitSet) set.clone());
    }
}
