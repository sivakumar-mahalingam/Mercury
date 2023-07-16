package mercury.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.*;

public class MergeArraysUDF extends GenericUDF {
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

        for (DeferredObject argument : arguments) {
            Object array = argument.get();
            List<?> list = (List<?>) ObjectInspectorUtils.copyToStandardObject(array, standardListObjectInspector);
            mergedList.addAll(list);
        }

        HashSet<Object> set = new HashSet<>(mergedList);
        mergedList.clear();
        mergedList.addAll(set);

        // Sort the ArrayList using a custom Comparator
        Collections.sort(mergedList, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                String s1 = o1.toString();
                String s2 = o2.toString();
                return s1.compareTo(s2);
            }
        });


        return mergedList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "MergeArraysUDF";
    }

}

