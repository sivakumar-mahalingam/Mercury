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

public class ArraySortTest {
    private ArraySort udf;

    @Before
    public void before() {
        udf = new ArraySort();
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithNoArguments() throws UDFArgumentException {
        udf.initialize(new ObjectInspector[0]);
    }

    @Test
    public void testInitializeWithOneArgument() throws UDFArgumentException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        udf.initialize(new ObjectInspector[]{listOi});
    }

    @Test
    public void testInitializeWithTwoArguments() throws UDFArgumentException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        udf.initialize(new ObjectInspector[]{listOi, stringOi});
    }

    /*
     * array_sort([1, 3, 2], asc) = [1, 2, 3]
     */
    @Test
    public void testEvaluateIntegersAscending() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(3);
        one.add(2);

        List<Integer> two = new ArrayList<Integer>();
        two.add(1);
        two.add(2);
        two.add(3);

        String sortOrder = "asc";

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(sortOrder)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }

    /*
     * array_sort([1, 3, 2], desc) = [3, 2, 1]
     */
    @Test
    public void testEvaluateIntegersDescending() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(3);
        one.add(2);

        List<Integer> two = new ArrayList<Integer>();
        two.add(3);
        two.add(2);
        two.add(1);

        String sortOrder = "desc";

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(sortOrder)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }

    /*
     * array_sort([1, 3, 2]) = [1, 2, 3]
     */
    @Test
    public void testEvaluateIntegersNoArgument() throws HiveException {
        ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<Integer> one = new ArrayList<Integer>();
        one.add(1);
        one.add(3);
        one.add(2);

        List<Integer> two = new ArrayList<Integer>();
        two.add(1);
        two.add(2);
        two.add(3);

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }

    /*
     * array_sort([a, c, b], asc) = [a, b, c]
     */
    @Test
    public void testEvaluateStringsAscending() throws HiveException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<String> one = new ArrayList<String>();
        one.add("a");
        one.add("c");
        one.add("b");

        List<String> two = new ArrayList<String>();
        two.add("a");
        two.add("b");
        two.add("c");

        String sortOrder = "asc";

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(sortOrder)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }

    /*
     * array_sort([a, c, b], desc) = [c, b, a]
     */
    @Test
    public void testEvaluateStringsDescending() throws HiveException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<String> one = new ArrayList<String>();
        one.add("a");
        one.add("c");
        one.add("b");

        List<String> two = new ArrayList<String>();
        two.add("c");
        two.add("b");
        two.add("a");

        String sortOrder = "desc";

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(sortOrder)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }

    /*
     * array_sort([a, c, b]) = [a, b, c]
     */
    @Test
    public void testEvaluateStringsNoArgument() throws HiveException {
        ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
        StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, stringOi});

        List<String> one = new ArrayList<String>();
        one.add("a");
        one.add("c");
        one.add("b");

        List<String> two = new ArrayList<String>();
        two.add("a");
        two.add("b");
        two.add("c");

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one)});
        assertEquals(3, resultOi.getListLength(result));
        assertEquals(two, resultOi.getList(result));
    }
}
