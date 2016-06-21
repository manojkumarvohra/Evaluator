# Evaluator
This UDF evaluates the input expression to a boolean

-----
Usage
-----
*evaluate_to_boolean(<String> expression, input0, input1, ....input-n)*

_Input indexing start from 0_
---------
Examples
--------
hive> SELECT evaluate_to_boolean(':0*:2==:1',1,2,2);

*true*

hive> SELECT evaluate_to_boolean(':0 between :1 and :2',current_timestamp(),current_date(), current_timestamp());

*true*

hive> SELECT evaluate_to_boolean('((:3 != :4) AND (:0 between :1 and :2))',current_timestamp(),current_date(), current_timestamp(), 12,12);

*false*

------------
Installation
------------

- checkout the repository
- make the package
- add the jar (without dependencies) to hive
- create temporary/permanent function evaluate_to_boolean as 'com.bigdata.hive.udf.impl.BooleanExpressionEvaluatorUDF'
