package strd.storage;

/**
 * User: light
 * Date: 9/2/13
 * Time: 4:04 PM
 */
public class FieldDeclaration {

//    public final int[] selectInputRef;
    public final int matrixOutputNum;
    public final FieldFunction fn;
    public final FieldFilterImpl[] filters;

    public FieldDeclaration(/*int[] selectInputRef,*/
                            int matrixOutputNum,
                            FieldFilterImpl[] filters,
                            FieldFunction fn) {

//        this.selectInputRef = selectInputRef;
        this.matrixOutputNum = matrixOutputNum;
        this.filters = filters;
        this.fn = fn;
    }


}
