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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for generating programs in the Enumerable (functional)
 * style.
 */
public class EnumUtils {

  private EnumUtils() {}

  static final boolean BRIDGE_METHODS = true;

  static final List<ParameterExpression> NO_PARAMS =
      ImmutableList.of();

  static final List<Expression> NO_EXPRS =
      ImmutableList.of();

  static final List<String> LEFT_RIGHT =
      ImmutableList.of("left", "right");

  /** Declares a method that overrides another method. */
  static MethodDeclaration overridingMethodDecl(Method method,
      Iterable<ParameterExpression> parameters,
      BlockStatement body) {
    return Expressions.methodDecl(
        method.getModifiers() & ~Modifier.ABSTRACT,
        method.getReturnType(),
        method.getName(),
        parameters,
        body);
  }

  static Type javaClass(
      JavaTypeFactory typeFactory, RelDataType type) {
    final Type clazz = typeFactory.getJavaClass(type);
    return clazz instanceof Class ? clazz : Object[].class;
  }

  static List<Type> fieldTypes(
      final JavaTypeFactory typeFactory,
      final List<? extends RelDataType> inputTypes) {
    return new AbstractList<Type>() {
      public Type get(int index) {
        return EnumUtils.javaClass(typeFactory, inputTypes.get(index));
      }
      public int size() {
        return inputTypes.size();
      }
    };
  }

  static List<RelDataType> fieldRowTypes(
      final RelDataType inputRowType,
      final List<? extends RexNode> extraInputs,
      final List<Integer> argList) {
    final List<RelDataTypeField> inputFields = inputRowType.getFieldList();
    return new AbstractList<RelDataType>() {
      public RelDataType get(int index) {
        final int arg = argList.get(index);
        return arg < inputFields.size()
            ? inputFields.get(arg).getType()
            : extraInputs.get(arg - inputFields.size()).getType();
      }
      public int size() {
        return argList.size();
      }
    };
  }

  static Expression joinSelector(JoinRelType joinType, PhysType physType,
      List<PhysType> inputPhysTypes) {
    // A parameter for each input.
    final List<ParameterExpression> parameters = new ArrayList<>();

    // Generate all fields.
    final List<Expression> expressions = new ArrayList<>();
    final int outputFieldCount = physType.getRowType().getFieldCount();
    for (Ord<PhysType> ord : Ord.zip(inputPhysTypes)) {
      final PhysType inputPhysType =
          ord.e.makeNullable(joinType.generatesNullsOn(ord.i));
      // If input item is just a primitive, we do not generate specialized
      // primitive apply override since it won't be called anyway
      // Function<T> always operates on boxed arguments
      final ParameterExpression parameter =
          Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()),
              EnumUtils.LEFT_RIGHT.get(ord.i));
      parameters.add(parameter);
      if (expressions.size() == outputFieldCount) {
        // For instance, if semi-join needs to return just the left inputs
        break;
      }
      final int fieldCount = inputPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        Expression expression =
            inputPhysType.fieldReference(parameter, i,
                physType.getJavaFieldType(expressions.size()));
        if (joinType.generatesNullsOn(ord.i)) {
          expression =
              Expressions.condition(
                  Expressions.equal(parameter, Expressions.constant(null)),
                  Expressions.constant(null),
                  expression);
        }
        expressions.add(expression);
      }
    }
    return Expressions.lambda(
        Function2.class,
        physType.record(expressions),
        parameters);
  }

  /** Converts from internal representation to JDBC representation used by
   * arguments of user-defined functions. For example, converts date values from
   * {@code int} to {@link java.sql.Date}. */
  private static Expression fromInternal(Expression e, Class<?> targetType) {
    if (e == ConstantUntypedNull.INSTANCE) {
      return e;
    }
    if (!(e.getType() instanceof Class)) {
      return e;
    }
    if (targetType.isAssignableFrom((Class) e.getType())) {
      return e;
    }
    if (targetType == java.sql.Date.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, e);
    }
    if (targetType == java.sql.Time.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, e);
    }
    if (targetType == java.sql.Timestamp.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, e);
    }
    if (Primitive.is(e.type)
        && Primitive.isBox(targetType)) {
      // E.g. e is "int", target is "Long", generate "(long) e".
      return Expressions.convert_(e,
          Primitive.ofBox(targetType).primitiveClass);
    }
    return e;
  }

  public static List<Expression> fromInternal(Class<?>[] targetTypes,
      List<Expression> expressions) {
    final List<Expression> list = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      list.add(fromInternal(expressions.get(i), targetTypes[i]));
    }
    return list;
  }

  static Type fromInternal(Type type) {
    if (type == java.sql.Date.class || type == java.sql.Time.class) {
      return int.class;
    }
    if (type == java.sql.Timestamp.class) {
      return long.class;
    }
    return type;
  }

  static Type toInternal(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case DATE:
    case TIME:
      return type.isNullable() ? Integer.class : int.class;
    case TIMESTAMP:
      return type.isNullable() ? Long.class : long.class;
    default:
      return null; // we don't care; use the default storage type
    }
  }

  static List<Type> internalTypes(List<? extends RexNode> operandList) {
    return Util.transform(operandList, node -> toInternal(node.getType()));
  }

  static Expression enforce(final Type storageType,
      final Expression e) {
    if (storageType != null && e.type != storageType) {
      if (e.type == java.sql.Date.class) {
        if (storageType == int.class) {
          return Expressions.call(BuiltInMethod.DATE_TO_INT.method, e);
        }
        if (storageType == Integer.class) {
          return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, e);
        }
      } else if (e.type == java.sql.Time.class) {
        if (storageType == int.class) {
          return Expressions.call(BuiltInMethod.TIME_TO_INT.method, e);
        }
        if (storageType == Integer.class) {
          return Expressions.call(BuiltInMethod.TIME_TO_INT_OPTIONAL.method, e);
        }
      } else if (e.type == java.sql.Timestamp.class) {
        if (storageType == long.class) {
          return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG.method, e);
        }
        if (storageType == Long.class) {
          return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, e);
        }
      }
    }
    return e;
  }

  /** Transforms a JoinRelType to Linq4j JoinType. **/
  static JoinType toLinq4jJoinType(JoinRelType joinRelType) {
    switch (joinRelType) {
    case INNER:
      return JoinType.INNER;
    case LEFT:
      return JoinType.LEFT;
    case RIGHT:
      return JoinType.RIGHT;
    case FULL:
      return JoinType.FULL;
    case SEMI:
      return JoinType.SEMI;
    case ANTI:
      return JoinType.ANTI;
    }
    throw new IllegalStateException(
        "Unable to convert " + joinRelType + " to Linq4j JoinType");
  }

  static Expression getNotNullLiteralValueExpression(
      RexLiteral literal, Type javaClass) {
    final Object value;
    switch (literal.getType().getSqlTypeName()) {
    case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class, Expressions.constant(bd.toString()));
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value = literal.getValueAs(Integer.class);
      javaClass = int.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      value = literal.getValueAs(Long.class);
      javaClass = long.class;
      break;
    case CHAR:
    case VARCHAR:
      value = literal.getValueAs(String.class);
      break;
    case BINARY:
    case VARBINARY:
      return Expressions.new_(ByteString.class,
          Expressions.constant(literal.getValueAs(byte[].class), byte[].class));
    case SYMBOL:
      value = literal.getValueAs(Enum.class);
      javaClass = value.getClass();
      break;
    default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value2 = literal.getValueAs(Comparable.class);
      if (primitive != null && value2 instanceof Number) {
        value = primitive.number((Number) value2);
      } else {
        value = value2;
      }
    }
    return Expressions.constant(value, javaClass);
  }

  static Expression doConvert(Expression operand, Type toType) {
    final Type fromType = operand.getType();
    return doConvert(operand, fromType, toType);
  }

  static Expression doConvert(Expression operand, Type fromType,
      Type toType) {
    if (fromType.equals(toType)) {
      return operand;
    }
    // E.g. from "Short" to "int".
    // Generate "x.intValue()".
    final Primitive toPrimitive = Primitive.of(toType);
    final Primitive toBox = Primitive.ofBox(toType);
    final Primitive fromBox = Primitive.ofBox(fromType);
    final Primitive fromPrimitive = Primitive.of(fromType);
    final boolean fromNumber = fromType instanceof Class
        && Number.class.isAssignableFrom((Class) fromType);
    if (fromType == String.class) {
      if (toPrimitive != null) {
        switch (toPrimitive) {
        case CHAR:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          // Generate "SqlFunctions.toShort(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        default:
          // Generate "Short.parseShort(x)".
          return Expressions.call(
              toPrimitive.boxClass,
              "parse" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        }
      }
      if (toBox != null) {
        switch (toBox) {
        case CHAR:
          // Generate "SqlFunctions.toCharBoxed(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toBox.primitiveName) + "Boxed",
              operand);
        default:
          // Generate "Short.valueOf(x)".
          return Expressions.call(
              toBox.boxClass,
              "valueOf",
              operand);
        }
      }
    }
    if (toPrimitive != null) {
      if (fromPrimitive != null) {
        // E.g. from "float" to "double"
        return Expressions.convert_(
            operand, toPrimitive.primitiveClass);
      }
      if (fromNumber || fromBox == Primitive.CHAR) {
        // Generate "x.shortValue()".
        return Expressions.unbox(operand, toPrimitive);
      } else {
        // E.g. from "Object" to "short".
        // Generate "SqlFunctions.toShort(x)"
        return Expressions.call(
            SqlFunctions.class,
            "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
            operand);
      }
    } else if (fromNumber && toBox != null) {
      // E.g. from "Short" to "Integer"
      // Generate "x == null ? null : Integer.valueOf(x.intValue())"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.box(
              Expressions.unbox(operand, toBox),
              toBox));
    } else if (fromPrimitive != null && toBox != null) {
      // E.g. from "int" to "Long".
      // Generate Long.valueOf(x)
      // Eliminate primitive casts like Long.valueOf((long) x)
      if (operand instanceof UnaryExpression) {
        UnaryExpression una = (UnaryExpression) operand;
        if (una.nodeType == ExpressionType.Convert
            || Primitive.of(una.getType()) == toBox) {
          return Expressions.box(una.expression, toBox);
        }
      }
      return Expressions.box(operand, toBox);
    } else if (fromType == java.sql.Date.class) {
      if (toType == int.class) {
        return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand);
      } else if (toType == Integer.class) {
        return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, operand);
      } else {
        return Expressions.convert_(operand, toType);
      }
    } else if (toType == java.sql.Date.class) {
      // E.g. from "int" or "Integer" to "java.sql.Date",
      // generate "SqlFunctions.internalToDate".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand);
      } else {
        return Expressions.convert_(operand, java.sql.Date.class);
      }
    } else if (toType == java.sql.Time.class) {
      // E.g. from "int" or "Integer" to "java.sql.Time",
      // generate "SqlFunctions.internalToTime".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand);
      } else {
        return Expressions.convert_(operand, java.sql.Time.class);
      }
    } else if (toType == java.sql.Timestamp.class) {
      // E.g. from "long" or "Long" to "java.sql.Timestamp",
      // generate "SqlFunctions.internalToTimestamp".
      if (isA(fromType, Primitive.LONG)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method,
            operand);
      } else {
        return Expressions.convert_(operand, java.sql.Timestamp.class);
      }
    } else if (toType == BigDecimal.class) {
      if (fromBox != null) {
        // E.g. from "Integer" to "BigDecimal".
        // Generate "x == null ? null : new BigDecimal(x.intValue())"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.new_(
                BigDecimal.class,
                Expressions.unbox(operand, fromBox)));
      }
      if (fromPrimitive != null) {
        // E.g. from "int" to "BigDecimal".
        // Generate "new BigDecimal(x)"
        return Expressions.new_(
            BigDecimal.class, operand);
      }
      // E.g. from "Object" to "BigDecimal".
      // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.call(
              SqlFunctions.class,
              "toBigDecimal",
              operand));
    } else if (toType == String.class) {
      if (fromPrimitive != null) {
        switch (fromPrimitive) {
        case DOUBLE:
        case FLOAT:
          // E.g. from "double" to "String"
          // Generate "SqlFunctions.toString(x)"
          return Expressions.call(
              SqlFunctions.class,
              "toString",
              operand);
        default:
          // E.g. from "int" to "String"
          // Generate "Integer.toString(x)"
          return Expressions.call(
              fromPrimitive.boxClass,
              "toString",
              operand);
        }
      } else if (fromType == BigDecimal.class) {
        // E.g. from "BigDecimal" to "String"
        // Generate "x.toString()"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.call(
                SqlFunctions.class,
                "toString",
                operand));
      } else {
        // E.g. from "BigDecimal" to "String"
        // Generate "x == null ? null : x.toString()"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.call(operand, "toString"));
      }
    }
    return Expressions.convert_(operand, toType);
  }

  private static boolean isA(Type fromType, Primitive primitive) {
    return Primitive.of(fromType) == primitive
        || Primitive.ofBox(fromType) == primitive;
  }

  public static Expression doCast(
          final JavaTypeFactory typeFactory,
          final Expression root,
          final RelDataType sourceType,
          final RelDataType targetType,
          Expression operand) {
    Expression convert = null;
    switch (targetType.getSqlTypeName()) {
    case ANY:
      convert = operand;
      break;
    case DATE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand);
        break;
      case TIMESTAMP:
        convert = Expressions.convert_(
            Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                int.class);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIME:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(BuiltInMethod.STRING_TO_TIME.method, operand);
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP:
        convert = Expressions.convert_(
          Expressions.call(
              BuiltInMethod.FLOOR_MOD.method,
              operand,
              Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
              int.class);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIME_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand);
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIME_TO_STRING.method,
                    operand)),
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
                    Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                operand));
      }
      break;
    case TIMESTAMP:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(BuiltInMethod.STRING_TO_TIMESTAMP.method, operand);
        break;
      case DATE:
        convert = Expressions.multiply(
            Expressions.convert_(operand, long.class),
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
        break;
      case TIME:
        convert = Expressions.add(
            Expressions.multiply(
                Expressions.convert_(
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                    long.class),
                Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                Expressions.convert_(operand, long.class));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(
            BuiltInMethod.STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            operand);
        break;
      case DATE:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.multiply(
                        Expressions.convert_(operand, long.class),
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)))),
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.add(
                        Expressions.multiply(
                            Expressions.convert_(
                                Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                long.class),
                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                                Expressions.convert_(operand, long.class)))),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                    operand));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
      }
      break;
    case BOOLEAN:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(
            BuiltInMethod.STRING_TO_BOOLEAN.method,
            operand);
      }
      break;
    case CHAR:
    case VARCHAR:
      final SqlIntervalQualifier interval = sourceType.getIntervalQualifier();
      switch (sourceType.getSqlTypeName()) {
      case DATE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                 BuiltInMethod.UNIX_DATE_TO_STRING.method,
                 operand));
        break;
      case TIME:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIME_TO_STRING.method,
                operand));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                operand));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                operand,
                Expressions.constant(interval.timeUnitRange)));
        break;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                operand,
                Expressions.constant(interval.timeUnitRange),
                Expressions.constant(
                    interval.getFractionalSecondPrecision(
                        typeFactory.getTypeSystem()))));
        break;
      case BOOLEAN:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.BOOLEAN_TO_STRING.method,
                operand));
        break;
      }
    }
    if (convert == null) {
      convert = doConvert(operand, typeFactory.getJavaClass(targetType));
    }
    // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
    // longer than n.
    boolean pad = false;
    boolean truncate = true;
    switch (targetType.getSqlTypeName()) {
    case CHAR:
    case BINARY:
      pad = true;
      // fall through
    case VARCHAR:
    case VARBINARY:
      final int targetPrecision = targetType.getPrecision();
      if (targetPrecision >= 0) {
        switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
          // If this is a widening cast, no need to truncate.
          final int sourcePrecision = sourceType.getPrecision();
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              <= 0) {
            truncate = false;
          }
          // If this is a widening cast, no need to pad.
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              >= 0) {
            pad = false;
          }
          // fall through
        default:
          if (truncate || pad) {
            convert = Expressions.call(
                pad
                ? BuiltInMethod.TRUNCATE_OR_PAD.method
                : BuiltInMethod.TRUNCATE.method,
                convert,
                Expressions.constant(targetPrecision));
          }
        }
      }
      break;
    case TIMESTAMP:
      int targetScale = targetType.getScale();
      if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
        targetScale = 0;
      }
      if (targetScale < sourceType.getScale()) {
        convert = Expressions.call(
                          BuiltInMethod.ROUND_LONG.method,
                          convert,
                          Expressions.constant(
                                  (long) Math.pow(10, 3 - targetScale)));
      }
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      switch (sourceType.getSqlTypeName().getFamily()) {
      case NUMERIC:
        final BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;
        final BigDecimal divider = BigDecimal.ONE;
        convert = multiplyDivide(convert, multiplier, divider);
      }
    }
    return scaleIntervalToNumber(sourceType, targetType, convert);
  }

  /** Multiplies an expression by a constant and divides by another constant,
   * optimizing appropriately.
   *
   * <p>For example, {@code multiplyDivide(e, 10, 1000)} returns
   * {@code e / 100}. */
  public static Expression multiplyDivide(Expression e, BigDecimal multiplier,
      BigDecimal divider) {
    if (multiplier.equals(BigDecimal.ONE)) {
      if (divider.equals(BigDecimal.ONE)) {
        return e;
      }
      return Expressions.divide(e,
          Expressions.constant(divider.intValueExact()));
    }
    final BigDecimal x =
        multiplier.divide(divider, RoundingMode.UNNECESSARY);
    switch (x.compareTo(BigDecimal.ONE)) {
    case 0:
      return e;
    case 1:
      return Expressions.multiply(e, Expressions.constant(x.intValueExact()));
    case -1:
      return multiplyDivide(e, BigDecimal.ONE, x);
    default:
      throw new AssertionError();
    }
  }

  private static Expression scaleIntervalToNumber(
          RelDataType sourceType,
          RelDataType targetType,
          Expression operand) {
    switch (targetType.getSqlTypeName().getFamily()) {
    case NUMERIC:
      switch (sourceType.getSqlTypeName()) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        // Scale to the given field.
        final BigDecimal multiplier = BigDecimal.ONE;
        final BigDecimal divider =
            sourceType.getSqlTypeName().getEndUnit().multiplier;
        return multiplyDivide(operand, multiplier, divider);
      }
    }
    return operand;
  }
}

// End EnumUtils.java
