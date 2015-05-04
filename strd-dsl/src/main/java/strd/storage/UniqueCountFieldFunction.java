package strd.storage;

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 9/9/13
 * Time: 12:52 PM
 */
public class UniqueCountFieldFunction extends UniqueCountFunctionBase {

    public final int valueColumn;
    private final int storeLimit;

    public UniqueCountFieldFunction(int valueColumn, int storeLimit ) {
        this.valueColumn = valueColumn;

        this.storeLimit = storeLimit;
    }

    @Override
    public boolean canSkip() {
        return storeLimit != -1;
    }

    @Override
    public boolean storeSkip( UniqueCountFieldFunctionState state ) {
        return storeLimit != -1 && storeLimit > state.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void append(SelectInputSource cols, UniqueCountFieldFunctionState state) {
        state.add(cols.get(valueColumn));
    }



}
