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
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Value Constructor Implementor
 */
class ValueConstructorImplementor extends AbstractRexCallImplementor {

  ValueConstructorImplementor() {
    super(NullPolicy.NONE, false);
  }

  @Override String getVariableName() {
    return "value_constructor";
  }

  @Override Expression implementSafe(RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    SqlKind kind = call.getOperator().getKind();
    final BlockBuilder blockBuilder = translator.getBlockBuilder();
    switch (kind) {
    case MAP_VALUE_CONSTRUCTOR:
      Expression map =
          blockBuilder.append(
              "map",
              Expressions.new_(LinkedHashMap.class),
              false);
      for (int i = 0; i < argValueList.size(); i++) {
        Expression key = argValueList.get(i++);
        Expression value = argValueList.get(i);
        blockBuilder.add(
            Expressions.statement(
                Expressions.call(
                    map,
                    BuiltInMethod.MAP_PUT.method,
                    Expressions.box(key),
                    Expressions.box(value))));
      }
      return map;
    case ARRAY_VALUE_CONSTRUCTOR:
      Expression lyst =
          blockBuilder.append(
              "list",
              Expressions.new_(ArrayList.class),
              false);
      for (Expression value : argValueList) {
        blockBuilder.add(
            Expressions.statement(
                Expressions.call(
                    lyst,
                    BuiltInMethod.COLLECTION_ADD.method,
                    Expressions.box(value))));
      }
      return lyst;
    default:
      throw new AssertionError("unexpected: " + kind);
    }
  }
}

// End ValueConstructorImplementor.java
