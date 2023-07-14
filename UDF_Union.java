package com.hive.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

public class MergeArraysUDF extends GenericUDF {

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

        StandardListObjectInspector listObjectInspector = (StandardListObjectInspector) arguments[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<Object> mergedList = new ArrayList<>();

        for (DeferredObject argument : arguments) {
            Object array = argument.get();
            List<?> list = (List<?>) ObjectInspectorUtils.copyToStandardObject(array, listObjectInspector);
            mergedList.addAll(list);
        }

        return mergedList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "MergeArraysUDF";
    }

}
