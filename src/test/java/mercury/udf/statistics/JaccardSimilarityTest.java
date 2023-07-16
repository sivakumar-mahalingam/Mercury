package mercury.udf.statistics;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JaccardSimilarityTest {
    private JaccardSimilarity udf;

    @Before
    public void before() {
        udf = new JaccardSimilarity();
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithNoArguments() throws UDFArgumentException {
        udf.initialize(new ObjectInspector[0]);
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithOneArgument() throws UDFArgumentException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        udf.initialize(new ObjectInspector[]{listOi});
    }

    @Test
    public void testInitializeWithTwoArguments() throws UDFArgumentException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        udf.initialize(new ObjectInspector[]{listOi, listOi});
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithThreeArguments() throws UDFArgumentException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        udf.initialize(new ObjectInspector[]{listOi, listOi, listOi});
    }

    /*
     * A = {1, 2, 3, 5, 7}
     * B = {1, 2, 4, 8, 9}
     * Jaccard Similarity Coefficient = 0.25
     */
    @Test
    public void testEvaluateWithTwoArrays() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        udf.initialize(new ObjectInspector[]{listOi, listOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(2);
        one.add(3);
        one.add(5);
        one.add(7);

        List<Integer> two = new ArrayList<Integer>();
        two.add(1);
        two.add(2);
        two.add(4);
        two.add(8);
        two.add(9);

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(one), new GenericUDF.DeferredJavaObject(two)});
        assertEquals(0.25, result);
    }
}
