package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 8:15 PM
 */
public class CountFieldFunction extends FieldFunction<CountFieldFunctionState,Long> {

    public static final int[] EMPTY = new int[0];

    @Override
    public CountFieldFunctionState init() {
        return new CountFieldFunctionState();
    }

    @Override
    public void append(SelectInputSource cols, CountFieldFunctionState state) {
        state.count = state.count + 1;
    }

    @Override
    public void merge(CountFieldFunctionState state1, CountFieldFunctionState state2, CountFieldFunctionState stateOut) {
        stateOut.count = state1.count + state2.count;
    }

    @Override
    public Long fetch(CountFieldFunctionState state) {
        return state.count;
    }
}
