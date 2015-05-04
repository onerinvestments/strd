package strd.storage;

import com.twitter.ostrich.stats.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: penkov
 * Date: 26.03.14
 * Time: 17:46
 */
public class SumFieldFunction extends FieldFunction<SumFieldFunctionState, Long> {


    private final Logger log = LoggerFactory.getLogger(getClass());

    public final int valueColumn;

    public SumFieldFunction(int valueColumn) {
        this.valueColumn = valueColumn;
    }

    @Override
    public SumFieldFunctionState init() {
        return new SumFieldFunctionState();
    }

    @Override
    public void append(SelectInputSource cols, SumFieldFunctionState state) {
        state.state = state.state + ((Number) cols.get(valueColumn)).longValue();
    }

    @Override
    public void merge(SumFieldFunctionState state1, SumFieldFunctionState state2, SumFieldFunctionState stateOut) {
        stateOut.state = state1.state + state2.state;
    }

    @Override
    public Long fetch(SumFieldFunctionState state) {
        return state.state;
    }
}
