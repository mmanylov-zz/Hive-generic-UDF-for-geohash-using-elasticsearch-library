package com.mmanylov;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.elasticsearch.common.geo.GeoPoint;


// hadoop fs -put target/HiveGeohashUDF-1.0-SNAPSHOT.jar /scripts
// CREATE FUNCTION geohash AS 'com.mmanylov.GetGeohash' using JAR 'hdfs:///scripts/HiveGeohashUDF-1.0-SNAPSHOT.jar';
// select lat, lng, geohash(lat, lng) as GetGeohash from weather_parquet limit 10;
/**
 * GetGeohash(double latitude, double longitude) is a function to get 4-symbol geohash.
 * See explain extended annotation below to read more about how this UDF works
 *
 */
@UDFType(deterministic = true)
// Description of the UDF
@Description(
        name="GetGeohash UDF",
        value="returns a 4-symbol geohash of provided lat and lng values",
        extended="select GetGeohash(latitude, longitude) from weather limit 10;"
)
public class GetGeohash extends GenericUDF {

    /**
     * Converters for retrieving the arguments to the UDF.
     */
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("_FUNC_ expects exactly 2 arguments");
        }


        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "A double argument was expected but an argument of type " + arguments[i].getTypeName()
                                + " was given.");

            }

            // Now that we have made sure that the argument is of primitive type, we can get the primitive
            // category
            PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i])
                    .getPrimitiveCategory();

            if (primitiveCategory != PrimitiveCategory.DOUBLE
                    && primitiveCategory != PrimitiveCategory.VOID) {
                throw new UDFArgumentTypeException(i,
                        "A double argument was expected but an argument of type " + arguments[i].getTypeName()
                                + " was given.");

            }
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }

        // We will be returning a Text object
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);

        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        double latitude = (double) converters[0].convert(arguments[0].get());
        double longitude = (double) converters[1].convert(arguments[1].get());

        GeoPoint point = new GeoPoint(latitude, longitude);
        String geohash = point.geohash();
        return geohash.substring(0, 4);
	//return "geohash here";
    }

    @Override
    public String getDisplayString(String[] arguments) {
        assert (arguments.length == 2);
        return "GetGeohash(" + arguments[0] + ", " + arguments[1] + ")";
    }
}
