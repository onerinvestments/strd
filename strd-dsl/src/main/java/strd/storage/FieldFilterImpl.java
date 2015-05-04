package strd.storage;

import strd.node.impl.ValueFilter;

/**
 * User: light
 * Date: 09/12/13
 * Time: 01:52
 */
public class FieldFilterImpl {

    public final int dataStream;
    public final ValueFilter filter;

    public FieldFilterImpl(ValueFilter filter, int dataStream) {
        this.dataStream = dataStream;
        this.filter = filter;
    }

    public boolean isIncludes(SelectInputSource values) {
        return filter.isIncludesObject(values.get(dataStream));
    }
}
