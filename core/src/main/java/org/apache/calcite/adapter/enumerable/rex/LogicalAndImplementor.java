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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Logical And Implementor
 *
 * <p>If any of the arguments are false, result is false;
 * else if any arguments are null, result is null;
 * else true.</p>
 */
class LogicalAndImplementor extends AbstractRexCallImplementor {

  LogicalAndImplementor() {
    super(NullPolicy.NONE, true);
  }

  @Override String getVariableName() {
    return "logical_and";
  }

  @Override public RexToLixTranslator.Result implement(final RexToLixTranslator translator,
       final RexCall call, final List<RexToLixTranslator.Result> arguments) {
    final List<Expression> argIsNullList = new ArrayList<>();
    for (RexToLixTranslator.Result result: arguments) {
      argIsNullList.add(result.isNullVariable);
    }
    final List<Expression> nullAsTrue =
        arguments.stream()
            .map(result ->
                Expressions.condition(
                    result.isNullVariable,
                    RexImpTable.TRUE_EXPR,
                    result.valueVariable))
            .collect(Collectors.toList());
    final Expression hasFalse =
        Expressions.not(Expressions.foldAnd(nullAsTrue));
    final Expression hasNull = Expressions.foldOr(argIsNullList);
    final Expression callExpression =
        Expressions.condition(
            hasFalse,
            RexImpTable.BOXED_FALSE_EXPR,
            Expressions.condition(
                hasNull,
                RexImpTable.NULL_EXPR,
                RexImpTable.BOXED_TRUE_EXPR));
    final RexImpTable.NullAs nullAs = translator.isNullable(call)
            ? RexImpTable.NullAs.NULL : RexImpTable.NullAs.NOT_POSSIBLE;
    final Expression valueExpression = nullAs.handle(callExpression);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(),
            translator.getBlockBuilder().newName(getVariableName() + "_value"));
    final Expression isNullExpression = translator.checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE,
            translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
    translator.getBlockBuilder().add(
        Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    translator.getBlockBuilder().add(
        Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    return new RexToLixTranslator.Result(isNullVariable, valueVariable);
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    return null;
  }
}

// End LogicalAndImplementor.java
