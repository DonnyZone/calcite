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

import java.lang.reflect.Type;
import java.util.List;

/** Implementor for the {@code IS_NOT_FALSE} SQL operator. */
class IsNotFalseImplementor extends AbstractRexCallImplementor {

  IsNotFalseImplementor() {
    super(NullPolicy.STRICT, false);
  }

  @Override String getVariableName() {
    return "is_not_false";
  }

  @Override Expression genIsNullExpression(final RexToLixTranslator translator,
      final Expression condition,
      final ParameterExpression value,
      List<Expression> argValueList) {
    return RexImpTable.FALSE_EXPR;
  }

  @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
    return Expressions.constant(true, type);
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    return Expressions.notEqual(argValueList.get(0), RexImpTable.FALSE_EXPR);
  }
}

// End IsNotFalseImplementor.java
