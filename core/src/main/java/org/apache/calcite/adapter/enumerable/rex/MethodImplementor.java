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
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

/** Implementor for a function that generates calls to a given method. */
class MethodImplementor extends AbstractRexCallImplementor {

  private final Method method;

  MethodImplementor(Method method, NullPolicy nullPolicy, boolean harmonize) {
    super(nullPolicy, harmonize);
    this.method = method;
  }

  @Override String getVariableName() {
    return "method_call";
  }

  @Override Expression implementSafe(RexToLixTranslator translator,
      RexCall call, List<Expression> argValueList) {
    List<Expression> unboxValueList = argValueList;
    if (nullPolicy == NullPolicy.STRICT) {
      unboxValueList = argValueList.stream()
          .map(RexImpTable.NullAs.NOT_POSSIBLE::handle)
              .collect(Collectors.toList());
    }
    final Expression expression;
    if (Modifier.isStatic(method.getModifiers())) {
      expression = Expressions.call(method, unboxValueList);
    } else {
      expression = Expressions.call(unboxValueList.get(0), method,
          Util.skip(unboxValueList, 1));
    }
    final Type returnType =
        translator.getTypeFactory().getJavaClass(call.getType());
    return Types.castIfNecessary(returnType, expression);
  }
}

// End MethodImplementor.java
