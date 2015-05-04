package strd.storage;

import strd.runtime.PipeValue;

import java.util.Collections;
import java.util.List;

/**
 * User: light
 * Date: 9/2/13
 * Time: 3:42 PM
 */
public abstract class FieldFunction<X extends FieldFunctionState, R> {

    public abstract X init();


    public boolean storeSkip(X state) {
        return false;
    }

    public boolean canSkip() {
        return false;
    }

    public abstract void append(SelectInputSource cols, X state);

    public abstract void merge(X state1, X state2, X stateOut);

    public abstract R fetch(X state);

    public Object fetchUnsafe(Object state) {
        return fetch((X) state);
    }

}
