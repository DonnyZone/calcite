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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Type;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_CATALOG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_PATH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_ROLE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SYSTEM_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.USER;

class RexCallSystemFunctionImplementor extends RexCallAbstractImplementor {

  RexCallSystemFunctionImplementor() {
    super(null);
  }

  @Override String getVariableName() {
    return "system_func";
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
    final SqlOperator op = call.getOperator();
    final Expression root = translator.getRoot();
    if (op == CURRENT_USER
        || op == SESSION_USER
            || op == USER) {
      return Expressions.call(BuiltInMethod.USER.method, root);
    } else if (op == SYSTEM_USER) {
      return Expressions.call(BuiltInMethod.SYSTEM_USER.method, root);
    } else if (op == CURRENT_PATH
        || op == CURRENT_ROLE
            || op == CURRENT_CATALOG) {
      // By default, the CURRENT_ROLE and CURRENT_CATALOG functions return the
      // empty string because a role or a catalog has to be set explicitly.
      return Expressions.constant("");
    } else if (op == CURRENT_TIMESTAMP) {
      return Expressions.call(BuiltInMethod.CURRENT_TIMESTAMP.method, root);
    } else if (op == CURRENT_TIME) {
      return Expressions.call(BuiltInMethod.CURRENT_TIME.method, root);
    } else if (op == CURRENT_DATE) {
      return Expressions.call(BuiltInMethod.CURRENT_DATE.method, root);
    } else if (op == LOCALTIMESTAMP) {
      return Expressions.call(BuiltInMethod.LOCAL_TIMESTAMP.method, root);
    } else if (op == LOCALTIME) {
      return Expressions.call(BuiltInMethod.LOCAL_TIME.method, root);
    } else {
      throw new AssertionError("unknown function " + op);
    }
  }
}

// End RexCallSystemFunctionImplementor.java
