package columnar;

public class IntegerValue<Integer> extends ValueClass {

    public IntegerValue(Integer val) {
        value = val;
    }

    public Integer getValue() {
        return (Integer)value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}