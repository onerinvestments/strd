package strd.storage;

import lmbrd.zn.util.PrimitiveBits;

import java.io.ByteArrayOutputStream;

/**
 * User: light
 * Date: 01/12/13
 * Time: 09:08
 */
public class FieldFunctionStates {
    private final FieldFunctionState[] states;

    public FieldFunctionStates(FieldFunctionState[] states) {
        this.states = states;
    }
    
    public FieldFunctionStates(FieldDeclaration[] decls) {
        this.states = new FieldFunctionState[decls.length];
        for (FieldDeclaration decl : decls) {
            this.states[decl.matrixOutputNum] = decl.fn.init();
        }
    }

    public int getCount() {
        return states.length;
    }

    public FieldFunctionStates(int count) {
        this.states = new FieldFunctionState[count];
    }

    public FieldFunctionState get(int i) {
        return states[i];
    }

    public void mergeWith( FieldFunctionStates s2, FieldDeclaration[] decls ) {
        for (FieldDeclaration decl : decls) {
            int n = decl.matrixOutputNum;
            decl.fn.merge( states[n], s2.states[n], states[n] );
        }
    }

    public static FieldFunctionState forType(int i) {
        switch (i){
            case ValueFieldFunctionState.ID: return new ValueFieldFunctionState();
            case UniqueCountFieldFunctionState.ID:  return new UniqueCountFieldFunctionState();
            case CountFieldFunctionState.ID: return new CountFieldFunctionState();
            case FetchByComparableFieldFunctionState.ID: return new FetchByComparableFieldFunctionState();
            case MergeHistogramFieldFunctionState.ID: return new MergeHistogramFieldFunctionState();
            case SumFieldFunctionState.ID: return new SumFieldFunctionState();
            case MaxFieldFunctionState.ID: return new MaxFieldFunctionState();
            default:
                throw new RuntimeException("Unsupported fieldFunctionState:" + i);
        }
    }

    public FieldFunctionStates deserialize( byte[] buf ) {
        final PrimitiveBits.DataBuf dataBuf = new PrimitiveBits.DataBuf(buf);
        for (FieldFunctionState state : states) {
            state.read(dataBuf);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public boolean isAlive(FieldDeclaration[] skippable) {
        for (FieldDeclaration field : skippable) {
            int n = field.matrixOutputNum;
            if (field.fn.storeSkip(get(n))) {
                return false;
            }
        }

        return true;
    }

    public void set(int colNum, FieldFunctionState state) {
        states[colNum] = state;
    }

    public int estimatedSize() {
        int sum = 0;
        for (FieldFunctionState state : states) {
            sum += state.estimatedSize();
        }
        return sum;
    }

    public byte[] serialize() {
        ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
        for (FieldFunctionState state : states) {
            state.write(byteBuf);
        }

//        fieldValues.foreach(fRef => buf.get(fRef.fNum).write(byteBuf))
//        val array = byteBuf.toByteArray
//
//        throw new UnsupportedOperationException();
        return byteBuf.toByteArray();
    }

}
