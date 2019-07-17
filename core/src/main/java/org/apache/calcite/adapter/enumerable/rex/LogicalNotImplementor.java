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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Logical Not Implementor
 *
 * <p>If any of the arguments are false, result is true;
 * else if any arguments are null, result is null;
 * else false.</p>
 */
class LogicalNotImplementor extends AbstractRexCallImplementor {

  LogicalNotImplementor() {
    super(NullPolicy.NONE, true);
  }

  @Override String getVariableName() {
    return "logical_not";
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    final Expression result =
        Expressions.call(BuiltInMethod.NOT.method, argValueList);
    final Type returnType =
        translator.getTypeFactory().getJavaClass(call.getType());
    return RexToLixTranslator.convert(result, returnType);
  }
}

// End LogicalNotImplementor.java
