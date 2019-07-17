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
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/** Implementor for the SQL {@code CASE} operator. */
class CaseImplementor extends AbstractRexCallImplementor {

  CaseImplementor() {
    super(NullPolicy.STRICT, false);
  }

  @Override String getVariableName() {
    return "case_when";
  }

  @Override Expression getCondition(List<Expression> argIsNullList) {
    if (argIsNullList.size() == 0) {
      return RexImpTable.FALSE_EXPR;
    }
    // Only need to check tester expressions
    final List<Expression> conditionArgs = new ArrayList<>();
    for (int i = 0; i < argIsNullList.size(); i = i + 2) {
      // the "else" clause
      if (i != argIsNullList.size() - 1) {
        conditionArgs.add(argIsNullList.get(i));
      }
    }
    return Expressions.foldOr(conditionArgs);
  }

  @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
    final int size = argValueList.size();
    if (size % 2 == 0) {
      return RexImpTable.getDefaultValue(type);
    }
    return RexToLixTranslator.convert(
        argValueList.get(size - 1), type);
  }

  @Override Expression genIsNullExpression(final RexToLixTranslator translator,
      final Expression condition, final ParameterExpression value,
      final List<Expression> argValueList) {
    Expression tmp = RexImpTable.TRUE_EXPR;
    final int size = argValueList.size();
    if (size % 2 == 1) {
      tmp = translator.checkNull(argValueList.get(size - 1));
    }
    return Expressions.condition(
        condition,
        tmp,
        translator.checkNull(value));
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    return implementRecurse(translator, call, argValueList, 0);
  }

  private Expression implementRecurse(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList, int i) {
    final Type returnType =
        translator.getTypeFactory().getJavaClass(call.getType());
    if (i == argValueList.size() - 1) {
      // the "else" clause
      return RexToLixTranslator.convert(
          argValueList.get(i), returnType);
    } else {
      Expression ifTrue =
          RexToLixTranslator.convert(argValueList.get(i + 1), returnType);
      Expression ifFalse =
          RexToLixTranslator.convert(
              implementRecurse(translator, call, argValueList, i + 2),
              returnType);
      Expression test = argValueList.get(i);
      return ifTrue == null || ifFalse == null
          ? Util.first(ifTrue, ifFalse)
          : Expressions.condition(test, ifTrue, ifFalse);
    }
  }
}

// End CaseImplementor.java
