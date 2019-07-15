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
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rex.RexCall;

import java.util.List;

class RexCallUnaryImplementor extends RexCallAbstractImplementor {
  private final ExpressionType expressionType;

  RexCallUnaryImplementor(ExpressionType expressionType, NullPolicy nullPolicy) {
    super(nullPolicy);
    this.expressionType = expressionType;
  }

  @Override String getVariableName() {
    return "unary_call";
  }

  @Override Expression implementNotNull(InnerVisitor translator,
        RexCall call, List<Expression> argValueList) {
    final Expression argValue = argValueList.get(0);
    final UnaryExpression e = Expressions.makeUnary(expressionType, argValue);
    if (e.type.equals(argValue.type)) {
      return e;
    }
    return Expressions.convert_(e, argValue.type);
  }

}

// End RexCallUnaryImplementor.java
