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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

class RexCallPeriodNameImplementor extends RexCallMethodNameImplementor {
  private final BuiltInMethod timestampMethod;
  private final BuiltInMethod dateMethod;

  RexCallPeriodNameImplementor(String methodName, BuiltInMethod timestampMethod,
      BuiltInMethod dateMethod, NullPolicy nullPolicy, boolean harmonize) {
    super(methodName, nullPolicy, harmonize);
    this.timestampMethod = timestampMethod;
    this.dateMethod = dateMethod;
  }

  @Override String getVariableName() {
    return "periodName";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    Expression operand = argValueList.get(0);
    final RelDataType type = call.operands.get(0).getType();
    switch (type.getSqlTypeName()) {
      case TIMESTAMP:
        return getExpression(translator, operand, timestampMethod);
      case DATE:
        return getExpression(translator, operand, dateMethod);
      default:
        throw new AssertionError("unknown type " + type);
    }
  }

  private Expression getExpression(InnerVisitor translator,
      Expression operand, BuiltInMethod builtInMethod) {
    final MethodCallExpression locale =
        Expressions.call(BuiltInMethod.LOCALE.method, translator.getRoot());
    return Expressions.call(builtInMethod.method.getDeclaringClass(),
        builtInMethod.method.getName(), operand, locale);
  }
}

// End RexCallPeriodNameImplementor.java
