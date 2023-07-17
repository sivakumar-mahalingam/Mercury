package mercury.udf.statistics;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashSet;
import java.util.List;

/**
 * UDF for getting jaccard similarity coefficient
 *
 * @author: Sivakumar Mahalingam
 */

@Description(name = "jaccard_similarity", value = "_FUNC_(a,b) - Returns jaccard similarity coefficient of two lists")
public class JaccardSimilarity extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("JaccardSimilarityUDF requires exactly 2 arguments");
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!arguments[i].getCategory().equals(ObjectInspector.Category.LIST)) {
                throw new UDFArgumentException("Argument " + i + " must be a list");
            }
        }

        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<Object> list1 = (List<Object>) arguments[0].get();
        List<Object> list2 = (List<Object>) arguments[1].get();

        int intersectionSize = 0;
        int unionSize = 0;

        HashSet<Object> hashSet = new HashSet<>();

        for (Object element : list1) {
            if (list2.contains(element)) {
                intersectionSize++;
            }
        }

        hashSet.addAll(list1);
        hashSet.addAll(list2);

        unionSize = hashSet.size();

        double jaccardSimilarity = (double) intersectionSize / (double) unionSize;

        return jaccardSimilarity;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "JaccardSimilarity";
    }
}
