package columnar;

import java.lang.*;

public class IntegerValue extends ValueClass<Integer> {

    public Integer getValue() {
        return value;
    }

    public IntegerValue(int ival) {
        value = ival;
    }
}
