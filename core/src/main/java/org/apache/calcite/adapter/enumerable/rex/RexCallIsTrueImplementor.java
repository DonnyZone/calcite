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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Type;
import java.util.List;

class RexCallIsTrueImplementor extends RexCallAbstractImplementor {

  RexCallIsTrueImplementor() {
    super(null, false);
  }

  @Override String getVariableName() {
    return "is_true";
  }

  Expression getIsNull(List<Expression> argIsNullList) {
    return RexCallImpTable.FALSE_EXPR;
  };

  @Override Expression getValueExpression(Type returnType, RexCall call,
      ParameterExpression isNull, Expression callValue) {
    return callValue;
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    return Expressions.equal(argValueList.get(0), RexCallImpTable.TRUE_EXPR);
  }
}

// End RexCallIsTrueImplementor.java
