package strd.storage;

import com.twitter.ostrich.stats.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: penkov
 * Date: 26.03.14
 * Time: 17:46
 */
public class MergeHistogramFieldFunction extends FieldFunction<MergeHistogramFieldFunctionState, Histogram> {


    private final Logger log = LoggerFactory.getLogger(getClass());

    public final int valueColumn;

    public MergeHistogramFieldFunction(int valueColumn) {
        this.valueColumn = valueColumn;
    }

    @Override
    public MergeHistogramFieldFunctionState init() {
        return new MergeHistogramFieldFunctionState();
    }

    @Override
    public void append(SelectInputSource cols, MergeHistogramFieldFunctionState state) {
        if (state.state==null) {
            state.state = (Histogram) cols.get(valueColumn);
        }
        else {
            state.state.merge((Histogram) cols.get(valueColumn));
        }
    }

    @Override
    public void merge(MergeHistogramFieldFunctionState state1, MergeHistogramFieldFunctionState state2, MergeHistogramFieldFunctionState stateOut) {
        if (state1.state == null && state2.state!=null) {
            stateOut.state = state2.state;
        }
        else if (state1.state!=null && state2.state == null) {
            stateOut.state = state1.state;
        }
        else if (state1.state == null && state2.state == null) {
            stateOut.state = null;
        }
        else {
            state1.state.merge(state2.state);
            stateOut.state = state1.state;
        }
    }

    @Override
    public Histogram fetch(MergeHistogramFieldFunctionState state) {
        return state.state;
    }
}
