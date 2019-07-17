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
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rex.RexCall;

import java.util.List;

/** Implementor for unary operators. */
class UnaryImplementor extends AbstractRexCallImplementor {
  private final ExpressionType expressionType;

  UnaryImplementor(ExpressionType expressionType, NullPolicy nullPolicy) {
    super(nullPolicy, false);
    this.expressionType = expressionType;
  }

  @Override String getVariableName() {
    return "unary_call";
  }

  @Override Expression implementSafe(RexToLixTranslator translator,
      RexCall call, List<Expression> argValueList) {
    final Expression argValue = argValueList.get(0);
    Expression unboxedArgValue = argValue;
    if (nullPolicy == NullPolicy.STRICT
        || nullPolicy == NullPolicy.SEMI_STRICT
            || nullPolicy == NullPolicy.ARG0) {
      unboxedArgValue = RexImpTable.NullAs.NOT_POSSIBLE.handle(argValue);
    }
    final UnaryExpression e = Expressions.makeUnary(expressionType, unboxedArgValue);
    Expression result = e;
    if (!e.type.equals(unboxedArgValue.type)) {
      // Certain unary operators do not preserve type. For example, the "-"
      // operator applied to a "byte" expression returns an "int".
      result = Expressions.convert_(e, unboxedArgValue.type);
    }
    return result;
  }
}

// End UnaryImplementor.java
