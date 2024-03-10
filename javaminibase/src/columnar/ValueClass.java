package columnar;
import java.lang.*;

public abstract class ValueClass<T> {
   protected T value;
   protected ValueClass(){} 
   protected abstract T getValue();
}
