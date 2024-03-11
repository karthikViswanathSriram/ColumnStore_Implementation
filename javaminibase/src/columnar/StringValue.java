package columnar;
import java.lang.*;

public class StringValue extends ValueClass<String> {

    public String getValue(){
        return value;
    }
    public StringValue(String sval) {
        value = sval;
    }
}
