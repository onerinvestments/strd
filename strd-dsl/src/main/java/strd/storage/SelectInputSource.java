package strd.storage;

import java.util.Arrays;

/**
 * User: light
 * Date: 09/12/13
 * Time: 01:33
 */
public class SelectInputSource {
    private final GrouppableFieldFunction[] fns;
    private final Object[] inputs;

    public SelectInputSource(int inputs, FieldDeclaration[] fns) {
        this.fns = new GrouppableFieldFunction[inputs];

        for (FieldDeclaration fd : fns) {
            if (fd.fn instanceof GrouppableFieldFunction) {
                GrouppableFieldFunction ff = (GrouppableFieldFunction) fd.fn;
                this.fns[ff.inputId()] = ff;
            }
        }

        this.inputs = new Object[inputs];
    }

    public void set(int index, Object value) {
        if (value == null) {
            throw new NullPointerException();
        }

        this.inputs[index] = value;
    }

    public Object get(int index) {
        Object o = inputs[index];

        if (o == null) {
            throw new NullPointerException("Null value for index: " + index);
        }

        return o;
    }

    public Object getForGroupper(int index) {
        GrouppableFieldFunction fn = fns[index];
        if ( fn == null ) {
            throw new NullPointerException("No function for index: " + index + " / " + Arrays.toString(fns) );
        }

        return fn.fetchForGroup(this);
    }

    public void clear() {
        Arrays.fill(inputs, null);
    }
}
