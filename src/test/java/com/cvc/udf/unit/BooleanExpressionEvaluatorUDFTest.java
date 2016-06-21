package com.cvc.udf.unit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

import com.bigdata.hive.udf.impl.BooleanExpressionEvaluatorUDF;

import model.DeferredArgument;

public class BooleanExpressionEvaluatorUDFTest {

	BooleanExpressionEvaluatorUDF udf = new BooleanExpressionEvaluatorUDF();

	@Test
	public void shouldEvaluateExpressionToTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>(":0==:1");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("2011-01-22");

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(true));
	}

	@Test
	public void shouldEvaluateExpressionToFalse() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>(":0!=:1");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("2011-01-22");

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(false));
	}

	@Test
	public void shouldEvaluateBetweenExpressionToTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>(":0 between :1 and :2");
		arguments[1] = new DeferredArgument<Integer>(1);
		arguments[2] = new DeferredArgument<Integer>(0);
		arguments[3] = new DeferredArgument<Integer>(1);

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(true));
	}

	@Test
	public void shouldEvaluateBetweenExpressionToFalse() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>(":0 between :1 and :2");
		arguments[1] = new DeferredArgument<Integer>(2);
		arguments[2] = new DeferredArgument<Integer>(0);
		arguments[3] = new DeferredArgument<Integer>(1);

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(false));
	}

	@Test
	public void shouldEvaluateLogicalExpressionToTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("((:0 between :1 and :2) AND (:0==:2))");
		arguments[1] = new DeferredArgument<Integer>(1);
		arguments[2] = new DeferredArgument<Integer>(0);
		arguments[3] = new DeferredArgument<Integer>(1);

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(true));
	}

	@Test
	public void shouldEvaluateLogicalExpressionToFalse() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("((:0 between :1 and :2) AND (:0==:1))");
		arguments[1] = new DeferredArgument<Integer>(1);
		arguments[2] = new DeferredArgument<Integer>(0);
		arguments[3] = new DeferredArgument<Integer>(1);

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(false));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void shouldEvaluateBetweenExpressionForTimestampDateToTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;

		udf.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("((:0 between :1 and :2))");

		Timestamp t1 = new Timestamp(2016, 6, 18, 12, 33, 22, 1);
		Timestamp t2 = new Timestamp(2016, 6, 17, 10, 59, 59, 122);
		Timestamp t3 = new Timestamp(2016, 6, 19, 12, 33, 22, 1);
		arguments[1] = new DeferredArgument<TimestampWritable>(new TimestampWritable(t1));
		arguments[2] = new DeferredArgument<TimestampWritable>(new TimestampWritable(t2));
		arguments[3] = new DeferredArgument<TimestampWritable>(new TimestampWritable(t3));

		BooleanWritable output = (BooleanWritable) udf.evaluate(arguments);

		assertThat(output.get(), is(true));
	}
}
