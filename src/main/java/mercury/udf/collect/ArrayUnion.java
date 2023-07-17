package mercury.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.*;

/**
 * UDF for combining multiple lists together
 *
 * @author: Sivakumar Mahalingam
 */

@Description(name = "array_union", value = "_FUNC_(a,b) - Returns a combined list of two or more lists")
public class ArrayUnion extends GenericUDF {
    private StandardListObjectInspector standardListObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentException("MergeArraysUDF requires at least 2 arguments");
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
        List<Object> mergedList = new ArrayList<>();
        HashSet<Object> hashSet = new HashSet<>();

        for (DeferredObject argument : arguments) {
            Object array = argument.get();
            List<?> list = (List<?>) ObjectInspectorUtils.copyToStandardObject(array, standardListObjectInspector);
            hashSet.addAll(list);
            mergedList = new ArrayList<>(hashSet);
        }

        return mergedList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "ArrayUnion";
    }
}
