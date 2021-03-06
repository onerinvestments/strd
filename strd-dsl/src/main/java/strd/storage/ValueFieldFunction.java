package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:52 PM
 */
public class ValueFieldFunction extends FieldFunction<ValueFieldFunctionState,Object> implements  GrouppableFieldFunction{
    private final int valueColumn;

    public ValueFieldFunction(int valueColumn) {
        this.valueColumn = valueColumn;
    }

    @Override
    public Object fetchForGroup(SelectInputSource columnValues) {
        return columnValues.get(valueColumn);
    }

    @Override
    public int inputId() {
        return valueColumn;
    }

    @Override
    public ValueFieldFunctionState init() {
        return new ValueFieldFunctionState();
    }

    @Override
    public void append(SelectInputSource cols, ValueFieldFunctionState state) {
        state.value = cols.get(valueColumn);
    }

    @Override
    public void merge(ValueFieldFunctionState state1, ValueFieldFunctionState state2, ValueFieldFunctionState stateOut) {
        if ( state1.value != null ) {
            stateOut.value = state1.value;
        } else {
            stateOut.value = state2.value;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object fetch(ValueFieldFunctionState state) {
        if (state.value == null) {
            throw new NullPointerException();
        }
        return  state.value;
    }

}
