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
import java.util.List;

/**
 * UDF for getting common elements from multiple lists
 *
 * @author: Sivakumar Mahalingam
 */

@Description(name = "array_intersection", value = "_FUNC_(a,b) - Returns a list which have common elements from two or more lists")
public class ArrayIntersection extends GenericUDF {
    private StandardListObjectInspector standardListObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentException("ArrayIntersection requires at least 2 arguments");
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
        List<Object> intersectionList = new ArrayList<>();

        for (DeferredObject argument : arguments) {
            Object array = argument.get();
            List<?> list = (List<?>) ObjectInspectorUtils.copyToStandardObject(array, standardListObjectInspector);
            if (intersectionList.size() == 0) {
                intersectionList.addAll(list);
            }
            intersectionList.retainAll(list);
        }

        return intersectionList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "ArrayIntersection";
    }
}
