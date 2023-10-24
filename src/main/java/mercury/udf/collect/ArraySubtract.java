package mercury.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * UDF for subtracting one list from another
 *
 * @author: Sivakumar Mahalingam
 */

@Description(name = "array_subtract", value = "_FUNC_(a,b) - Returns a list of items from one list and not in second")
public class ArraySubtract extends GenericUDF {
    private StandardListObjectInspector standardListObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentException("ArraySubtract requires at least 2 arguments");
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!arguments[i].getCategory().equals(ObjectInspector.Category.LIST)) {
                throw new UDFArgumentException("Argument " + i + " must be a list");
            }
        }

        standardListObjectInspector = (StandardListObjectInspector) arguments[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(standardListObjectInspector.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<Object> subtractList = new ArrayList<>();
        List<Object> list1 = (List<Object>) arguments[0].get();
        List<Object> list2 = (List<Object>) arguments[1].get();

        for (Object element : list1) {
            if (!list2.contains(element)) {
                subtractList.add(element);
            }
        }
        return subtractList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "ArraySubtract";
    }
}
