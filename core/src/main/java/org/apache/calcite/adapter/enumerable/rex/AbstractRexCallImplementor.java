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

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.Result;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implementation of the {@link RexCallImplementor} interface that
 * implements the extension methods.
 *
 * <p>It is not always safe to execute the {@link RexCall} directly due to
 * the special null arguments. Therefore, the generated code logic is
 * conditional correspondingly.</p>
 *
 * <p>For example, {@code a + b} will generate two declaration statements:
 * {@code
 *   final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b);
 *   final boolean xxx_isNull = (a_isNull || b_isNull) ? true : xxx_value == null;
 * }
 * </p>
 */
abstract class AbstractRexCallImplementor implements RexCallImplementor {

  final NullPolicy nullPolicy;
  private final boolean harmonize;

  AbstractRexCallImplementor(NullPolicy nullPolicy, boolean harmonize) {
    this.nullPolicy = nullPolicy;
    this.harmonize = harmonize;
  }

  @Override public Result implement(final RexToLixTranslator translator,
      final RexCall call, final List<Result> arguments) {
    final List<Expression> argIsNullList = new ArrayList<>();
    final List<Expression> argValueList = new ArrayList<>();
    for (Result result: arguments) {
      argIsNullList.add(result.isNullVariable);
      argValueList.add(result.valueVariable);
    }
    final Expression condition = getCondition(argIsNullList);
    final ParameterExpression valueVariable =
        genValueStatement(translator, call, argValueList, condition);
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE,
            translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
    final Expression isNullExpression = genIsNullExpression(
            translator, condition, valueVariable, argValueList);
    translator.getBlockBuilder().add(
        Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    return new Result(isNullVariable, valueVariable);
  }

  // variable name for code readability and trouble shooting convenience
  abstract String getVariableName();

  /**Figure out conditional expression based on {@code nullPolicy}*/
  Expression getCondition(final List<Expression> argIsNullList) {
    if (argIsNullList.size() == 0 || nullPolicy == null
        || nullPolicy == NullPolicy.NONE) {
      return RexImpTable.FALSE_EXPR;
    }
    if (nullPolicy == NullPolicy.ARG0) {
      return argIsNullList.get(0);
    }
    if (nullPolicy == NullPolicy.ANY) {
      return Expressions.foldAnd(argIsNullList);
    }
    return Expressions.foldOr(argIsNullList);
  }

  private ParameterExpression genValueStatement(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList,
      final Expression condition) {
    List<Expression> harmonizedArgValueList = argValueList;
    if (harmonize) {
      harmonizedArgValueList =
          harmonize(argValueList, translator, call);
    }
    final Expression callValue =
        implementSafe(translator, call, harmonizedArgValueList);
    final Expression valueExpression =
        Expressions.condition(
            condition,
            getIfTrue(callValue.getType(), argValueList),
            callValue);
    final ParameterExpression value =
        Expressions.parameter(callValue.getType(),
            translator.getBlockBuilder().newName(getVariableName() + "_value"));
    translator.getBlockBuilder().add(
        Expressions.declare(Modifier.FINAL, value, valueExpression));
    return value;
  }

  Expression getIfTrue(Type type, final List<Expression> argValueList) {
    return RexImpTable.getDefaultValue(type);
  }

  Expression genIsNullExpression(final RexToLixTranslator translator,
      final Expression condition, final ParameterExpression value,
      final List<Expression> argValueList) {
    return Expressions.condition(
        condition,
        RexImpTable.TRUE_EXPR,
        translator.checkNull(value));
  }

  /** Ensures that operands have identical type. */
  private List<Expression> harmonize(final List<Expression> argValueList,
       final RexToLixTranslator translator, final RexCall call) {
    int nullCount = 0;
    final List<RelDataType> types = new ArrayList<>();
    final RelDataTypeFactory typeFactory =
        translator.getRexBuilder().getTypeFactory();
    for (RexNode operand : call.getOperands()) {
      RelDataType type = operand.getType();
      type = toSql(typeFactory, type);
      if (translator.isNullable(operand)) {
        ++nullCount;
      } else {
        type = typeFactory.createTypeWithNullability(type, false);
      }
      types.add(type);
    }
    if (allSame(types)) {
      // Operands have the same nullability and type. Return them
      // unchanged.
      return argValueList;
    }
    final RelDataType type = typeFactory.leastRestrictive(types);
    if (type == null) {
      // There is no common type. Presumably this is a binary operator with
      // asymmetric arguments (e.g. interval / integer) which is not intended
      // to be harmonized.
      return argValueList;
    }
    assert (nullCount > 0) == type.isNullable();
    final Type javaClass =
        translator.getTypeFactory().getJavaClass(type);
    final List<Expression> harmonizedArgValues = new ArrayList<>();
    for (Expression argValue : argValueList) {
      harmonizedArgValues.add(
          RexToLixTranslator.convert(argValue, javaClass));
    }
    return harmonizedArgValues;
  }

  private RelDataType toSql(RelDataTypeFactory typeFactory, RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      final SqlTypeName typeName = type.getSqlTypeName();
      if (typeName != null && typeName != SqlTypeName.OTHER) {
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(typeName),
            type.isNullable());
      }
    }
    return type;
  }

  private <E> boolean allSame(List<E> list) {
    E prev = null;
    for (E e : list) {
      if (prev != null && !prev.equals(e)) {
        return false;
      }
      prev = e;
    }
    return true;
  }

  abstract Expression implementSafe(RexToLixTranslator translator,
      RexCall call, List<Expression> argValueList);
}

// End AbstractRexCallImplementor.java
