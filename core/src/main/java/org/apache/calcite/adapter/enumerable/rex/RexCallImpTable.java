/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable.rex;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.linq4j.tree.ExpressionType.Add;
import static org.apache.calcite.linq4j.tree.ExpressionType.Divide;
import static org.apache.calcite.linq4j.tree.ExpressionType.Equal;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Multiply;
import static org.apache.calcite.linq4j.tree.ExpressionType.Negate;
import static org.apache.calcite.linq4j.tree.ExpressionType.NotEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Subtract;
import static org.apache.calcite.linq4j.tree.ExpressionType.UnaryPlus;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DIFFERENCE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_DEPTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_KEYS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_PRETTY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_REMOVE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_STORAGE_SIZE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_TYPE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LEFT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MONTHNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REPEAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RIGHT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SOUNDEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ABS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ACOS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASCII;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN2;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CARDINALITY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_CATALOG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_PATH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_ROLE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DATETIME_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEFAULT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEGREES;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ELEMENT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.INITCAP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_A_SET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_A_SET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_EXISTS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_QUERY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_ANY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_EXPRESSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_DAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOG10;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MEMBER_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_EXCEPT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_INTERSECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_UNION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_UNION_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NEXT_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OVERLAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RADIANS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REINTERPRET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SLICE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.STRUCT_ACCESS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SYSTEM_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRIM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRUNCATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.USER;

/**
 * Null Safe Implementor Table
 */
public class RexCallImpTable {

  public static final RexCallImpTable INSTANCE = new RexCallImpTable();
  private final Map<SqlOperator, RexCallImplementor> map = new HashMap<>();

  private RexCallImpTable() {
    defineMethod(ROW, BuiltInMethod.ARRAY.method, NullPolicy.ANY);
    defineMethod(UPPER, BuiltInMethod.UPPER.method, NullPolicy.STRICT);
    defineMethod(LOWER, BuiltInMethod.LOWER.method, NullPolicy.STRICT);
    defineMethod(INITCAP,  BuiltInMethod.INITCAP.method, NullPolicy.STRICT);
    defineMethod(TO_BASE64, BuiltInMethod.TO_BASE64.method, NullPolicy.STRICT);
    defineMethod(FROM_BASE64, BuiltInMethod.FROM_BASE64.method, NullPolicy.STRICT);
    defineMethod(SUBSTRING, BuiltInMethod.SUBSTRING.method, NullPolicy.STRICT);
    defineMethod(LEFT, BuiltInMethod.LEFT.method, NullPolicy.STRICT);
    defineMethod(RIGHT, BuiltInMethod.RIGHT.method, NullPolicy.STRICT);
    defineMethod(REPLACE, BuiltInMethod.REPLACE.method, NullPolicy.STRICT);
    defineMethod(TRANSLATE3, BuiltInMethod.TRANSLATE3.method, NullPolicy.STRICT);
    defineMethod(CHR, "chr", NullPolicy.STRICT);
    defineMethod(CHARACTER_LENGTH, BuiltInMethod.CHAR_LENGTH.method, NullPolicy.STRICT);
    defineMethod(CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH.method, NullPolicy.STRICT);
    defineMethod(CONCAT, BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT);
    defineMethod(OVERLAY, BuiltInMethod.OVERLAY.method, NullPolicy.STRICT);
    defineMethod(POSITION, BuiltInMethod.POSITION.method, NullPolicy.STRICT);
    defineMethod(ASCII, BuiltInMethod.ASCII.method, NullPolicy.STRICT);
    defineMethod(REPEAT, BuiltInMethod.REPEAT.method, NullPolicy.STRICT);
    defineMethod(SPACE, BuiltInMethod.SPACE.method, NullPolicy.STRICT);
    defineMethod(SOUNDEX, BuiltInMethod.SOUNDEX.method, NullPolicy.STRICT);
    defineMethod(DIFFERENCE, BuiltInMethod.DIFFERENCE.method, NullPolicy.STRICT);
    defineMethod(REVERSE, BuiltInMethod.REVERSE.method, NullPolicy.STRICT);

    map.put(TRIM, new TrimImplementor());

    // logical
    map.put(AND, new LogicalAndImplementor());
    map.put(OR, new LogicalOrImplementor());
    map.put(NOT, new LogicalNotImplementor());

    // comparisons
    defineBinary(LESS_THAN, LessThan, NullPolicy.STRICT, "lt");
    defineBinary(LESS_THAN_OR_EQUAL, LessThanOrEqual, NullPolicy.STRICT, "le");
    defineBinary(GREATER_THAN, GreaterThan, NullPolicy.STRICT, "gt");
    defineBinary(GREATER_THAN_OR_EQUAL, GreaterThanOrEqual, NullPolicy.STRICT, "ge");
    defineBinary(EQUALS, Equal, NullPolicy.STRICT, "eq");
    defineBinary(NOT_EQUALS, NotEqual, NullPolicy.STRICT, "ne");

    // arithmetic
    defineBinary(PLUS, Add, NullPolicy.STRICT, "plus");
    defineBinary(MINUS, Subtract, NullPolicy.STRICT, "minus");
    defineBinary(MULTIPLY, Multiply, NullPolicy.STRICT, "multiply");
    defineBinary(DIVIDE, Divide, NullPolicy.STRICT, "divide");
    defineBinary(DIVIDE_INTEGER, Divide, NullPolicy.STRICT, "divide");
    defineUnary(UNARY_MINUS, Negate, NullPolicy.STRICT);
    defineUnary(UNARY_PLUS, UnaryPlus, NullPolicy.STRICT);

    defineMethod(MOD, "mod", NullPolicy.STRICT);
    defineMethod(EXP, "exp", NullPolicy.STRICT);
    defineMethod(POWER, "power", NullPolicy.STRICT);
    defineMethod(LN, "ln", NullPolicy.STRICT);
    defineMethod(LOG10, "log10", NullPolicy.STRICT);
    defineMethod(ABS, "abs", NullPolicy.STRICT);

    map.put(RAND, new RandImplementor());
    map.put(RAND_INTEGER, new RandIntegerImplementor());

    defineMethod(ACOS, "acos", NullPolicy.STRICT);
    defineMethod(ASIN, "asin", NullPolicy.STRICT);
    defineMethod(ATAN, "atan", NullPolicy.STRICT);
    defineMethod(ATAN2, "atan2", NullPolicy.STRICT);
    defineMethod(COS, "cos", NullPolicy.STRICT);
    defineMethod(COT, "cot", NullPolicy.STRICT);
    defineMethod(DEGREES, "degrees", NullPolicy.STRICT);
    defineMethod(RADIANS, "radians", NullPolicy.STRICT);
    defineMethod(ROUND, "sround", NullPolicy.STRICT);
    defineMethod(SIGN, "sign", NullPolicy.STRICT);
    defineMethod(SIN, "sin", NullPolicy.STRICT);
    defineMethod(TAN, "tan", NullPolicy.STRICT);
    defineMethod(TRUNCATE, "struncate", NullPolicy.STRICT);

    map.put(PI, new PiImplementor());

    // datetime
    map.put(DATETIME_PLUS, new DatetimeArithmeticImplementor());
    map.put(MINUS_DATE, new DatetimeArithmeticImplementor());

    map.put(EXTRACT, new ExtractImplementor());
    map.put(FLOOR,
            new FloorImplementor(
                BuiltInMethod.FLOOR.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
                BuiltInMethod.UNIX_DATE_FLOOR.method));
    map.put(CEIL,
            new FloorImplementor(
                BuiltInMethod.CEIL.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
                BuiltInMethod.UNIX_DATE_CEIL.method));

    defineMethod(LAST_DAY, "lastDay", NullPolicy.STRICT);
    map.put(DAYNAME,
            new PeriodNameImplementor("dayName",
                BuiltInMethod.DAYNAME_WITH_TIMESTAMP,
                BuiltInMethod.DAYNAME_WITH_DATE));
    map.put(MONTHNAME,
            new PeriodNameImplementor("monthName",
                BuiltInMethod.MONTHNAME_WITH_TIMESTAMP,
                BuiltInMethod.MONTHNAME_WITH_DATE));

    map.put(IS_NULL, new IsNullImplementor());
    map.put(IS_NOT_NULL, new IsNotNullImplementor());
    map.put(IS_TRUE, new IsTrueImplementor());
    map.put(IS_NOT_TRUE, new IsNotTrueImplementor());
    map.put(IS_FALSE, new IsFalseImplementor());
    map.put(IS_NOT_FALSE, new IsNotFalseImplementor());

    // LIKE and SIMILAR
    final MethodImplementor likeImplementor =
        new MethodImplementor(BuiltInMethod.LIKE.method, NullPolicy.STRICT, false);
    map.put(LIKE, likeImplementor);
    map.put(NOT_LIKE, likeImplementor);
    final MethodImplementor similarImplementor =
        new MethodImplementor(BuiltInMethod.SIMILAR.method, NullPolicy.STRICT, false);
    map.put(SIMILAR_TO, similarImplementor);
    map.put(NOT_SIMILAR_TO, similarImplementor);

    // POSIX REGEX
    final MethodImplementor posixRegexImplementor =
        new MethodImplementor(BuiltInMethod.POSIX_REGEX.method,
                NullPolicy.STRICT, false);
    map.put(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, posixRegexImplementor);
    map.put(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, posixRegexImplementor);
    map.put(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE,
            NotImplementor.of(posixRegexImplementor));
    map.put(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE,
            NotImplementor.of(posixRegexImplementor));

    // Multisets & arrays
    defineMethod(CARDINALITY, BuiltInMethod.COLLECTION_SIZE.method, NullPolicy.STRICT);
    defineMethod(SLICE, BuiltInMethod.SLICE.method, NullPolicy.NONE);
    defineMethod(ELEMENT, BuiltInMethod.ELEMENT.method, NullPolicy.STRICT);
    defineMethod(STRUCT_ACCESS, BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY);
    defineMethod(MEMBER_OF, BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE);
    final MethodImplementor isEmptyImplementor =
        new MethodImplementor(BuiltInMethod.IS_EMPTY.method, NullPolicy.NONE, false);
    map.put(IS_EMPTY, isEmptyImplementor);
    map.put(IS_NOT_EMPTY, NotImplementor.of(isEmptyImplementor));
    final MethodImplementor isASetImplementor =
        new MethodImplementor(BuiltInMethod.IS_A_SET.method, NullPolicy.NONE, false);
    map.put(IS_A_SET, isASetImplementor);
    map.put(IS_NOT_A_SET, NotImplementor.of(isASetImplementor));
    defineMethod(MULTISET_INTERSECT_DISTINCT,
            BuiltInMethod.MULTISET_INTERSECT_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_INTERSECT,
            BuiltInMethod.MULTISET_INTERSECT_ALL.method, NullPolicy.NONE);
    defineMethod(MULTISET_EXCEPT_DISTINCT,
            BuiltInMethod.MULTISET_EXCEPT_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_EXCEPT, BuiltInMethod.MULTISET_EXCEPT_ALL.method, NullPolicy.NONE);
    defineMethod(MULTISET_UNION_DISTINCT,
            BuiltInMethod.MULTISET_UNION_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_UNION, BuiltInMethod.MULTISET_UNION_ALL.method, NullPolicy.NONE);
    final MethodImplementor subMultisetImplementor =
            new MethodImplementor(BuiltInMethod.SUBMULTISET_OF.method, NullPolicy.NONE, false);
    map.put(SUBMULTISET_OF, subMultisetImplementor);
    map.put(NOT_SUBMULTISET_OF, NotImplementor.of(subMultisetImplementor));

    map.put(CASE, new CaseImplementor());
    map.put(COALESCE, new CoalesceImplementor());
    map.put(CAST, new CastImplementor());

    map.put(REINTERPRET, new ReinterpretImplementor());

    final RexCallImplementor value = new ValueConstructorImplementor();
    map.put(MAP_VALUE_CONSTRUCTOR, value);
    map.put(ARRAY_VALUE_CONSTRUCTOR, value);
    map.put(ITEM, new ItemImplementor());

    map.put(DEFAULT, new DefaultImplementor());

    // Sequences
    defineMethod(CURRENT_VALUE, BuiltInMethod.SEQUENCE_CURRENT_VALUE.method, NullPolicy.STRICT);
    defineMethod(NEXT_VALUE, BuiltInMethod.SEQUENCE_NEXT_VALUE.method, NullPolicy.STRICT);

    // Json Operators
    defineMethod(JSON_VALUE_EXPRESSION,
            BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT);
    defineMethod(JSON_EXISTS, BuiltInMethod.JSON_EXISTS.method, NullPolicy.ARG0);
    defineMethod(JSON_VALUE_ANY, BuiltInMethod.JSON_VALUE_ANY.method, NullPolicy.ARG0);
    defineMethod(JSON_QUERY, BuiltInMethod.JSON_QUERY.method, NullPolicy.ARG0);
    defineMethod(JSON_TYPE, BuiltInMethod.JSON_TYPE.method, NullPolicy.ARG0);
    defineMethod(JSON_DEPTH, BuiltInMethod.JSON_DEPTH.method, NullPolicy.ARG0);
    defineMethod(JSON_KEYS, BuiltInMethod.JSON_KEYS.method, NullPolicy.ARG0);
    defineMethod(JSON_PRETTY, BuiltInMethod.JSON_PRETTY.method, NullPolicy.ARG0);
    defineMethod(JSON_LENGTH, BuiltInMethod.JSON_LENGTH.method, NullPolicy.ARG0);
    defineMethod(JSON_REMOVE, BuiltInMethod.JSON_REMOVE.method, NullPolicy.ARG0);
    defineMethod(JSON_STORAGE_SIZE, BuiltInMethod.JSON_STORAGE_SIZE.method, NullPolicy.ARG0);
    defineMethod(JSON_OBJECT, BuiltInMethod.JSON_OBJECT.method, NullPolicy.NONE);
    defineMethod(JSON_ARRAY, BuiltInMethod.JSON_ARRAY.method, NullPolicy.NONE);
    map.put(IS_JSON_VALUE,
            new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method, NullPolicy.NONE, false));
    map.put(IS_JSON_OBJECT,
            new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method, NullPolicy.NONE, false));
    map.put(IS_JSON_ARRAY,
            new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method, NullPolicy.NONE, false));
    map.put(IS_JSON_SCALAR,
            new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method, NullPolicy.NONE, false));
    map.put(IS_NOT_JSON_VALUE,
            NotImplementor.of(
                new MethodImplementor(
                    BuiltInMethod.IS_JSON_VALUE.method,
                    NullPolicy.NONE,
                    false)));
    map.put(IS_NOT_JSON_OBJECT,
            NotImplementor.of(
                new MethodImplementor(
                    BuiltInMethod.IS_JSON_OBJECT.method,
                    NullPolicy.NONE,
                    false)));
    map.put(IS_NOT_JSON_ARRAY,
            NotImplementor.of(
                new MethodImplementor(
                    BuiltInMethod.IS_JSON_ARRAY.method,
                    NullPolicy.NONE,
                    false)));
    map.put(IS_NOT_JSON_SCALAR,
            NotImplementor.of(
                new MethodImplementor(
                    BuiltInMethod.IS_JSON_SCALAR.method,
                    NullPolicy.NONE,
                    false)));

    // System functions
    final SystemFunctionImplementor systemFunctionImplementor =
        new SystemFunctionImplementor();
    map.put(USER, systemFunctionImplementor);
    map.put(CURRENT_USER, systemFunctionImplementor);
    map.put(SESSION_USER, systemFunctionImplementor);
    map.put(SYSTEM_USER, systemFunctionImplementor);
    map.put(CURRENT_PATH, systemFunctionImplementor);
    map.put(CURRENT_ROLE, systemFunctionImplementor);
    map.put(CURRENT_CATALOG, systemFunctionImplementor);

    // Current time functions
    map.put(CURRENT_TIME, systemFunctionImplementor);
    map.put(CURRENT_TIMESTAMP, systemFunctionImplementor);
    map.put(CURRENT_DATE, systemFunctionImplementor);
    map.put(LOCALTIME, systemFunctionImplementor);
    map.put(LOCALTIMESTAMP, systemFunctionImplementor);
  }

  private void defineMethod(SqlOperator operator, Method method, NullPolicy nullPolicy) {
    RexCallImplementor implementor =
        new MethodImplementor(method, nullPolicy, false);
    map.put(operator, implementor);
  }

  private void defineMethod(SqlOperator operator, String methodName, NullPolicy nullPolicy) {
    RexCallImplementor implementor =
        new MethodNameImplementor(methodName, nullPolicy, false);
    map.put(operator, implementor);
  }

  private void defineBinary(SqlOperator operator, ExpressionType expressionType,
      NullPolicy nullPolicy, String backupMethodName) {
    RexCallImplementor implementor =
        new BinaryImplementor(nullPolicy, true, expressionType, backupMethodName);
    map.put(operator, implementor);
  }

  private void defineUnary(SqlOperator operator, ExpressionType expressionType,
      NullPolicy nullPolicy) {
    RexCallImplementor implementor =
        new UnaryImplementor(expressionType, nullPolicy);
    map.put(operator, implementor);
  }

  public static RexCallImplementor createNullSafeImplementor(
      final NotNullImplementor implementor,
      final NullPolicy nullPolicy,
      final boolean harmonize) {
    return new AbstractRexCallImplementor(nullPolicy, harmonize) {
      @Override String getVariableName() {
        return "not_null_udf";
      }

      @Override Expression implementSafe(
           RexToLixTranslator translator, RexCall call,
           List<Expression> argValueList) {
        return implementor.implement(translator, call, argValueList);
      }
    };
  }

  private static RexCallImplementor wrapAsNullSafeImplementor(
      final CallImplementor implementor) {
    return new AbstractRexCallImplementor(NullPolicy.NONE, false) {
      @Override String getVariableName() {
        return "udf";
      }

      @Override Expression implementSafe(RexToLixTranslator translator,
          RexCall call, List<Expression> argValueList) {
        return implementor.implement(translator, call, RexImpTable.NullAs.NULL);
      }
    };
  }

  public RexCallImplementor get(final SqlOperator operator) {
    if (operator instanceof SqlUserDefinedFunction) {
      org.apache.calcite.schema.Function udf =
          ((SqlUserDefinedFunction) operator).getFunction();
      if (!(udf instanceof ImplementableFunction)) {
        throw new IllegalStateException("User defined function " + operator
            + " must implement ImplementableFunction");
      }
      CallImplementor implementor =
          ((ImplementableFunction) udf).getImplementor();
      return wrapAsNullSafeImplementor(implementor);
    }
    return map.get(operator);
  }
}

// End RexCallImpTable.java
