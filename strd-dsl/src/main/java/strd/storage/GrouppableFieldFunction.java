package strd.storage;

/**
 * User: light
 * Date: 17/01/14
 * Time: 13:09
 */
public interface GrouppableFieldFunction {
    public Object fetchForGroup(SelectInputSource columnValues);
    public int inputId();
}
