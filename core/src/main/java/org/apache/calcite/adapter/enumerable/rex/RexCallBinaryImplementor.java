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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;

class RexCallBinaryImplementor extends RexCallAbstractImplementor {

  /** Types that can be arguments to comparison operators such as
   * {@code <}. */
  private static final List<Primitive> COMP_OP_TYPES =
          ImmutableList.of(Primitive.BYTE, Primitive.CHAR,
                  Primitive.SHORT,
                  Primitive.INT,
                  Primitive.LONG,
                  Primitive.FLOAT,
                  Primitive.DOUBLE);

  private static final List<SqlBinaryOperator> COMPARISON_OPERATORS =
          ImmutableList.of(
                  SqlStdOperatorTable.LESS_THAN,
                  SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                  SqlStdOperatorTable.GREATER_THAN,
                  SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
  private static final String METHOD_POSTFIX_FOR_ANY_TYPE = "Any";

  private ExpressionType expressionType;
  private final String backupMethodName;

  RexCallBinaryImplementor(ExpressionType expressionType,
      NullPolicy nullPolicy, String backupMethodName) {
    super(nullPolicy);
    this.expressionType = expressionType;
    this.backupMethodName = backupMethodName;
  }

  @Override String getVariableName() {
    return "binary_call";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    final Type returnType = translator.typeFactory.getJavaClass(call.getType());
    if (backupMethodName != null) {
      // If one or both operands have ANY type, use the late-binding backup method.
      if (anyAnyOperands(call)) {
        return callBackupMethodAnyType(argValueList);
      }
      final Type type0 = argValueList.get(0).getType();
      final Type type1 = argValueList.get(1).getType();
      final SqlBinaryOperator op = (SqlBinaryOperator) call.getOperator();
      final Primitive primitive = Primitive.ofBoxOr(type0);
      if (primitive == null || type1 == BigDecimal.class
          || COMPARISON_OPERATORS.contains(op) && !COMP_OP_TYPES.contains(primitive)) {
        return Expressions.call(SqlFunctions.class, backupMethodName, argValueList);
      }
    }
    return Types.castIfNecessary(returnType,
        Expressions.makeBinary(expressionType,
            argValueList.get(0), argValueList.get(1)));
  }

  /** Returns whether any of a call's operands have ANY type. */
  private boolean anyAnyOperands(RexCall call) {
    for (RexNode operand : call.operands) {
      if (operand.getType().getSqlTypeName() == SqlTypeName.ANY) {
        return true;
      }
    }
    return false;
  }

  private Expression callBackupMethodAnyType(List<Expression> expressions) {
    final String backupMethodNameForAnyType =
        backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE;
    // one or both of parameter(s) is(are) ANY type
    final Expression expression0 = maybeBox(expressions.get(0));
    final Expression expression1 = maybeBox(expressions.get(1));
    return Expressions.call(SqlFunctions.class, backupMethodNameForAnyType,
            expression0, expression1);
  }

  private Expression maybeBox(Expression expression) {
    final Primitive primitive = Primitive.of(expression.getType());
    if (primitive != null) {
      expression = Expressions.box(expression, primitive);
    }
    return expression;
  }
}

// End RexCallBinaryImplementor.java
