package columnar;

public class StringValue<String> extends ValueClass {

    public StringValue(String val) {
        value = val;
    }

    public String getValue() {
        return (String) value;
    }

    public java.lang.String toString() {
        return java.lang.String.valueOf(value);
    }
}
