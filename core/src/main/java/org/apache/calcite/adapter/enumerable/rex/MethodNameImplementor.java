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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;

import java.util.List;
import java.util.stream.Collectors;

/** Implementor for SQL functions that generates calls to a given method name.
 *
 * <p>Use this, as opposed to {@link MethodImplementor}, if the SQL function
 * is overloaded; then you can use one implementor for several overloads. */
class MethodNameImplementor extends AbstractRexCallImplementor {

  final String methodName;

  MethodNameImplementor(String methodName,
      NullPolicy nullPolicy, boolean harmonize) {
    super(nullPolicy, harmonize);
    this.methodName = methodName;
  }

  @Override String getVariableName() {
    return "method_name_call";
  }

  @Override Expression implementSafe(RexToLixTranslator translator,
      RexCall call, List<Expression> argValueList) {
    List<Expression> unboxValueList = argValueList;
    if (nullPolicy == NullPolicy.STRICT) {
      unboxValueList = argValueList.stream()
          .map(RexImpTable.NullAs.NOT_POSSIBLE::handle)
              .collect(Collectors.toList());
    }
    return Expressions.call(SqlFunctions.class, methodName, unboxValueList);
  }
}

// End MethodNameImplementor.java
