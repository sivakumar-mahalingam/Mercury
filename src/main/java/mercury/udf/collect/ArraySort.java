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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * UDF for combining multiple lists together
 *
 * @author: Sivakumar Mahalingam
 */

@Description(name = "array_sort", value = "_FUNC_(a,b) - Returns a sorted list in ascending orders")
public class ArraySort extends GenericUDF {
    private StandardListObjectInspector standardListObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentException("ArraySort requires at least 1 argument");
        } else if (arguments.length > 2) {
            throw new UDFArgumentException("ArraySort requires at most 2 arguments");
        } else if (arguments.length == 2) {
            if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) {
                throw new UDFArgumentException("Argument 1 must be a list");
            }
            if (!arguments[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
                throw new UDFArgumentException("Argument 2 must be a string");
            }
        }

        standardListObjectInspector = (StandardListObjectInspector) arguments[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(standardListObjectInspector.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<Object> sortedList = new ArrayList<>();

        List<?> list = (List<?>) ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), standardListObjectInspector);
        Object[] arrayObject = list.toArray();

        String sortOrder = "asc";
        if (arguments.length == 2) {
            sortOrder = (String) arguments[1].get();
            sortOrder = sortOrder.toLowerCase();
            if (!(sortOrder.equals("asc") || sortOrder.equals("desc"))) {
                throw new HiveException("Array sort order should be either 'asc' or 'desc'");
            }
        }

        if (sortOrder.equals("desc")) {
            Arrays.sort(arrayObject, Collections.reverseOrder());
        } else {
            Arrays.sort(arrayObject);
        }

        sortedList.addAll(List.of(arrayObject));

        return sortedList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "ArraySort";
    }
}
