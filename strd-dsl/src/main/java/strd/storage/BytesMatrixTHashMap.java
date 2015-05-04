package strd.storage;

import gnu.trove.map.hash.THashMap;
import lmbrd.zn.util.BytesHash;

/**
 * User: light
 * Date: 01/12/13
 * Time: 03:32
 */
public class BytesMatrixTHashMap extends THashMap<byte[], FieldFunctionStates> {

    public BytesMatrixTHashMap() {
    }

    public BytesMatrixTHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public BytesMatrixTHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    @Override
    protected int hash(Object notnull) {
        return BytesHash.instance.computeHashCode((byte[]) notnull);
    }

    @Override
    protected boolean equals(Object notnull, Object two) {
        return BytesHash.instance.equals((byte[])notnull, (byte[])two);
    }

}
