package mercury.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArraySubtractTest {
    private ArraySubtract udf;

    @Before
    public void before() {
        udf = new ArraySubtract();
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

    /*
     * {1, 2} - {1, 2} = {}
     */
    @Test
    public void testEvaluateWithSameElements() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, listOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(2);

        List<Integer> two = new ArrayList<Integer>();
        two.add(1);
        two.add(2);

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(two)});
        assertEquals(0, resultOi.getListLength(result));
    }

    /*
     * {1, 2, 3} - {1, 2, 4} = {3}
     */
    @Test
    public void testEvaluateWithTwoArrays() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, listOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(2);
        one.add(3);

        List<Integer> two = new ArrayList<Integer>();
        two.add(1);
        two.add(2);
        two.add(4);

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(two)});
        assertEquals(1, resultOi.getListLength(result));
        assertTrue(resultOi.getList(result).contains(3));
    }
}
