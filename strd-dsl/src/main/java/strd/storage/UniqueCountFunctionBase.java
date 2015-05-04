package strd.storage;

import gnu.trove.procedure.TObjectProcedure;

/**
 * User: light
 * Date: 09/12/13
 * Time: 16:11
 */
public abstract class UniqueCountFunctionBase extends FieldFunction<UniqueCountFieldFunctionState, Long> {
    @Override
    public UniqueCountFieldFunctionState init() {
        return new UniqueCountFieldFunctionState();
    }

    @Override
    public Long fetch(UniqueCountFieldFunctionState state) {
        return state.size();

    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(UniqueCountFieldFunctionState state1, UniqueCountFieldFunctionState state2, final UniqueCountFieldFunctionState stateOut) {

        if (state1.set == null && state1.state == null) {

            stateOut.set = state2.set;
            stateOut.state = state2.state;

        } else if (state2.set == null && state2.state == null) {

            stateOut.set = state1.set;
            stateOut.state = state1.state;

        } else if (state1.state != null && state2.state != null) {

            stateOut.state = state1.state.merge( state2.state );

        } else if (state1.set != null && state2.set != null) {
            if (state1.set.size() + state2.set.size() > 1000) {
                addToHyperLogLog(state1, stateOut);
                addToHyperLogLog(state2, stateOut);
            } else {
                if (state1.set.size() > state2.set.size()) {
                    state1.set.addAll(state2.set);
                    stateOut.set = state1.set;
                } else {
                    state2.set.addAll(state1.set);
                    stateOut.set = state2.set;
                }
            }
        } else if (state1.state != null) {

            stateOut.state = state1.state;
            addToHyperLogLog(state2, stateOut);

        } else if (state2.state != null) {
            stateOut.state = state2.state;
            addToHyperLogLog(state1, stateOut);

        } else {
            throw new IllegalStateException();
        }

    }

    private void addToHyperLogLog(UniqueCountFieldFunctionState state2, final UniqueCountFieldFunctionState stateOut) {
        state2.set.forEach(new TObjectProcedure() {
            @Override
            public boolean execute(Object object) {
                stateOut.addToHyperLogLog(object);
                return true;
            }
        });
        state2.set = null;
    }


}
