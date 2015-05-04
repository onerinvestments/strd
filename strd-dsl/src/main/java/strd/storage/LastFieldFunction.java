package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:33 PM
 */
public class LastFieldFunction extends FieldFunction<FetchByComparableFieldFunctionState, Object> {

    private static final Comparable ABS_MIN = new AbsMinType();

    public final int valueColumn;
    public final int orderColumn;

    public LastFieldFunction(int valueColumn, int orderColumn) {

        this.orderColumn = orderColumn;
        this.valueColumn = valueColumn;

    }

    @Override
    public FetchByComparableFieldFunctionState init() {
        FetchByComparableFieldFunctionState state = new FetchByComparableFieldFunctionState();
        state.order = ABS_MIN;

        return state;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void append(SelectInputSource cols, FetchByComparableFieldFunctionState state) {

        Comparable orderValue = (Comparable) cols.get(orderColumn);
        if (state.order.compareTo(orderValue) < 0) {
            state.order = orderValue;
            state.value = cols.get(valueColumn);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(FetchByComparableFieldFunctionState state1, FetchByComparableFieldFunctionState state2, FetchByComparableFieldFunctionState stateOut) {

        if (!state2.order.equals(ABS_MIN) && state1.order.compareTo(state2.order) < 0) {
            stateOut.value = state2.value;
            stateOut.order = state2.order;
        } else {
            stateOut.value = state1.value;
            stateOut.order = state1.order;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object fetch(FetchByComparableFieldFunctionState state) {
        return state.value;
    }


    private static class AbsMinType implements Comparable {
        @Override
        public boolean equals(Object obj) {
            return obj instanceof AbsMinType;
        }

        @Override
        public int compareTo(Object o) {
            return -1;
        }
    }
}
