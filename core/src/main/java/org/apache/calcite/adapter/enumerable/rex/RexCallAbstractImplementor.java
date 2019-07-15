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
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

abstract class RexCallAbstractImplementor implements RexCallImplementor {

  final NullPolicy nullPolicy;

  RexCallAbstractImplementor(NullPolicy nullPolicy) {
    this.nullPolicy = nullPolicy;
  }

  abstract String getVariableName();

  Expression getIsNull(List<Expression> argIsNullList) {
    if (argIsNullList.size()==0) {
      return RexCallImpTable.FALSE_EXPR;
    }
    if (nullPolicy == NullPolicy.ARG0 || argIsNullList.size() == 1) {
      return argIsNullList.get(0);
    }
    return Expressions.foldOr(argIsNullList);
  };

  Expression getValueWhenIsNull(Type returnType) {
    return RexToLixTranslator.convert(RexCallImpTable.NULL_EXPR, returnType);
  }

  @Override public RexNodeGenResult implement(InnerVisitor translator,
      RexCall call, List<RexNodeGenResult> arguments) {
    final List<Expression> argIsNullList = new ArrayList<>();
    final List<Expression> argValueList = new ArrayList<>();
    for (RexNodeGenResult result: arguments) {
      argIsNullList.add(result.getIsNull());
      argValueList.add(result.getValue());
    }
    ParameterExpression isNull = genIsNullStatement(translator, argIsNullList);
    ParameterExpression value =
        genValueStatement(translator, call, argValueList, isNull);
    return new RexNodeGenResult(isNull, value);
  }

  private ParameterExpression genIsNullStatement(final InnerVisitor translator,
      final List<Expression> argIsNullList) {
    final ParameterExpression isNull =
       Expressions.parameter(Boolean.TYPE,
           translator.list.newName("isNull_" + getVariableName()));
    Expression isNullExpression = getIsNull(argIsNullList);
    translator.list.add(Expressions.declare(Modifier.FINAL, isNull, isNullExpression));
    return isNull;
  }

  private ParameterExpression genValueStatement(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList,
          final ParameterExpression isNull) {
    final Type returnType = translator.typeFactory.getJavaClass(call.getType());
    final ParameterExpression value =
        Expressions.parameter(returnType,
            translator.list.newName("value_" + getVariableName()));
    Expression callValue =
        implementNotNull(translator, call, argValueList);
    Expression valueExpression = getValueExpression(returnType, call, isNull, callValue);
    translator.list.add(Expressions.declare(Modifier.FINAL, value, valueExpression));
    return value;
  }

  Expression getValueExpression(Type returnType, RexCall call,
       ParameterExpression isNull, Expression callValue) {
    return Expressions.condition(isNull, getValueWhenIsNull(returnType), callValue);
  }

  abstract Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList);
}

// End RexCallAbstractImplementor.java
