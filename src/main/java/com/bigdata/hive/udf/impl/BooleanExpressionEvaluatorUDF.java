package com.bigdata.hive.udf.impl;

import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

/*
 * @Author: Manoj Kumar Vohra
 * @Date: 18-Jun-2016
 */
public class BooleanExpressionEvaluatorUDF extends GenericUDF {

	private static final String FUNCTION_USAGE = "Invalid function usage: Correct Usage => FunctionName(<String> expression, operand1, operand2 ..... operandn)";
	private LinkedList<ObjectInspector> operandInspectors = new LinkedList<ObjectInspector>();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length < 2) {
			throw new UDFArgumentLengthException(FUNCTION_USAGE);
		} else {
			verifyExpressionInspector(arguments);
		}

		for (int i = 1; i < arguments.length; i++) {
			ObjectInspector inspector = arguments[i];
			if (inspector != null) {
				operandInspectors.add(inspector);
			}
		}

		return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("js");
		Object result = null;

		String expression = getTransformedExpressionWithValuesSubstituted(arguments);

		try {
			result = engine.eval(expression);
		} catch (ScriptException e) {
			throw new RuntimeException("ERROR: could not evaluate expression: " + expression + "\n" + e.getMessage());
		}

		if (result == null) {
			throw new RuntimeException("ERROR: could not evaluate expression to boolean: " + expression);
		}

		return new BooleanWritable((Boolean) result);
	}

	private String getTransformedExpressionWithValuesSubstituted(DeferredObject[] arguments)
			throws HiveException, UDFArgumentException {
		Object rawExpression = arguments[0].get();

		if (rawExpression == null) {
			throw new UDFArgumentException("expression cannot be null");
		}

		String transformedExpression = null;

		try {
			transformedExpression = PrimitiveObjectInspectorFactory.javaStringObjectInspector
					.getPrimitiveJavaObject(rawExpression);

			transformedExpression = transformBetweenExpression(transformedExpression);
			transformedExpression = substituteOperators(transformedExpression);
			transformedExpression = substituteOperands(transformedExpression, arguments);

		} catch (Exception exception) {
			throw new UDFArgumentException("Invalid expression" + exception.getMessage());
		}

		return transformedExpression;
	}

	private String substituteOperands(String transformedExpression, DeferredObject[] arguments) throws HiveException {

		LinkedList<String> operands = new LinkedList<String>();

		for (int i = 1; i < arguments.length; i++) {
			DeferredObject argument = arguments[i];
			if (argument != null) {
				String operandValue = getOperandValueForArgument(argument, i);
				operands.add(operandValue);
			}
		}

		for (int i = 0; i < operands.size(); i++) {

			Pattern operandReplacementPattern = Pattern.compile("([:]" + i + ")");
			StringBuffer stringBuffer = new StringBuffer();
			Matcher operandPatterMatcher = operandReplacementPattern.matcher(transformedExpression);

			while (operandPatterMatcher.find()) {
				String replacementString = operands.get(i);
				operandPatterMatcher.appendReplacement(stringBuffer, replacementString);
			}
			operandPatterMatcher.appendTail(stringBuffer);
			transformedExpression = stringBuffer.toString();
		}

		transformedExpression = fixEscapedColonDigits(transformedExpression);

		return transformedExpression;
	}

	private String fixEscapedColonDigits(String transformedExpression) {

		String returnValue = transformedExpression;
		Pattern escapeColonDigitPattern = Pattern.compile("([:])/([0-9])");
		StringBuffer stringBuffer = new StringBuffer();
		Matcher colonDigitMatcher = escapeColonDigitPattern.matcher(returnValue);

		while (colonDigitMatcher.find()) {
			String replacementString = colonDigitMatcher.group(1) + colonDigitMatcher.group(2);
			colonDigitMatcher.appendReplacement(stringBuffer, replacementString);
		}
		colonDigitMatcher.appendTail(stringBuffer);
		returnValue = stringBuffer.toString();

		return returnValue;
	}

	private String getOperandValueForArgument(DeferredObject argument, int index) throws HiveException {

		ObjectInspector argumentInspector = operandInspectors.get(index - 1);
		String returnValue = argument.get().toString();

		if ((argumentInspector instanceof StringObjectInspector || argumentInspector instanceof HiveCharObjectInspector
				|| argumentInspector instanceof HiveVarcharObjectInspector)) {
			returnValue = "'" + returnValue + "'";
		} else if ((argumentInspector instanceof DateObjectInspector
				|| argumentInspector instanceof TimestampObjectInspector
				|| argumentInspector instanceof HiveIntervalDayTimeObjectInspector
				|| argumentInspector instanceof HiveIntervalYearMonthObjectInspector)) {
			returnValue = "'" + returnValue + "'";
		}

		Pattern escapeColonDigitPattern = Pattern.compile("([:])([0-9])");
		StringBuffer stringBuffer = new StringBuffer();
		Matcher colonDigitMatcher = escapeColonDigitPattern.matcher(returnValue);

		while (colonDigitMatcher.find()) {
			String replacementString = colonDigitMatcher.group(1) + "/" + colonDigitMatcher.group(2);
			colonDigitMatcher.appendReplacement(stringBuffer, replacementString);
		}
		colonDigitMatcher.appendTail(stringBuffer);
		returnValue = stringBuffer.toString();

		return returnValue;
	}

	private String transformBetweenExpression(String transformedExpression) {

		String translatedExpression = transformedExpression;

		// :0 between :1 and :2
		Pattern betweenReplacementPattern = Pattern
				.compile("([:][0-9]+)\\s+[bB][eE][tT][wW][eE][eE][nN]\\s+([:][0-9]+)\\s+[aA][nN][dD]\\s+([:][0-9]+)");

		StringBuffer stringBuffer = new StringBuffer();

		Matcher betweenPatterMatcher = betweenReplacementPattern.matcher(translatedExpression);

		while (betweenPatterMatcher.find()) {
			String operand1 = betweenPatterMatcher.group(1);
			String operand2 = betweenPatterMatcher.group(2);
			String operand3 = betweenPatterMatcher.group(3);
			String replacementString = " " + operand1 + " >= " + operand2 + " and " + operand1 + " <= " + operand3
					+ " ";
			betweenPatterMatcher.appendReplacement(stringBuffer, replacementString);
		}
		betweenPatterMatcher.appendTail(stringBuffer);
		translatedExpression = stringBuffer.toString();

		return translatedExpression;
	}

	private String substituteOperators(String expression) {

		String translatedExpression = expression;

		Pattern andReplacementPattern = Pattern.compile("(\\s+[aA][nN][dD]\\s+)");
		Pattern orReplacementPattern = Pattern.compile("(\\s+[oO][rR]\\s+)");

		StringBuffer stringBuffer = new StringBuffer();
		Matcher operatorMatcher = andReplacementPattern.matcher(translatedExpression);

		while (operatorMatcher.find()) {
			String replacementString = " && ";
			operatorMatcher.appendReplacement(stringBuffer, replacementString);

		}
		operatorMatcher.appendTail(stringBuffer);
		translatedExpression = stringBuffer.toString();

		stringBuffer = new StringBuffer();
		operatorMatcher = orReplacementPattern.matcher(translatedExpression);

		while (operatorMatcher.find()) {
			String replacementString = " || ";
			operatorMatcher.appendReplacement(stringBuffer, replacementString);

		}
		operatorMatcher.appendTail(stringBuffer);
		translatedExpression = stringBuffer.toString();

		return translatedExpression;
	}

	private void verifyExpressionInspector(ObjectInspector[] arguments) throws UDFArgumentTypeException {
		ObjectInspector unitInspector = arguments[0];
		if (!(unitInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(0, "Only String is accepted for expression parameter but "
					+ unitInspector.getTypeName() + " is passed as first argument");
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "BooleanExpressionEvaluatorUDF: evaluates the passed expression against the parameters and returns boolean result";
	}
}
