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
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

public class RexCallReflectiveImplementor extends RexCallAbstractImplementor {
  protected final Method method;

  RexCallReflectiveImplementor(Method method, NullPolicy nullPolicy) {
    super(nullPolicy);
    this.method = method;
  }

  @Override
  String getVariableName() {
    return "reflective_" + method.getName();
  }

  @Override
  Expression implementNotNull(InnerVisitor translator,
      RexCall call, List<Expression> argValueList) {
    List<Expression> argValueList0 =
        CodegenUtil.fromInternal(method.getParameterTypes(), argValueList);
    if ((method.getModifiers() & Modifier.STATIC) != 0) {
      return Expressions.call(method, argValueList0);
    } else {
      // The UDF class must have a public zero-args constructor.
      // Assume that the validator checked already.
      final Expression target =
              Expressions.new_(method.getDeclaringClass());
      return Expressions.call(target, method, argValueList0);
    }
  }

}

// End RexCallReflectiveImplementor.java
