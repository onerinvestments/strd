package strd.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: penkov
 * Date: 26.03.14
 * Time: 17:46
 */
public class MaxFieldFunction extends FieldFunction<MaxFieldFunctionState, Long> {


    private final Logger log = LoggerFactory.getLogger(getClass());

    public final int valueColumn;

    public MaxFieldFunction(int valueColumn) {
        this.valueColumn = valueColumn;
    }

    @Override
    public MaxFieldFunctionState init() {
        return new MaxFieldFunctionState();
    }

    @Override
    public void append(SelectInputSource cols, MaxFieldFunctionState state) {
        state.state = Math.max(state.state, ((Number) cols.get(valueColumn)).longValue());
    }

    @Override
    public void merge(MaxFieldFunctionState state1, MaxFieldFunctionState state2, MaxFieldFunctionState stateOut) {
        stateOut.state = Math.max(state1.state, state2.state);
    }

    @Override
    public Long fetch(MaxFieldFunctionState state) {
        return state.state;
    }
}
