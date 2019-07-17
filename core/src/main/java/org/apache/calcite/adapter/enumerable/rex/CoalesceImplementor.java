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
import org.apache.calcite.util.Util;

import java.util.List;

/** Implementor for the SQL {@code COALESCE} operator. */
class CoalesceImplementor extends AbstractRexCallImplementor {

  CoalesceImplementor() {
    super(NullPolicy.NONE, false);
  }

  @Override String getVariableName() {
    return "coalesce";
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    return implementRecurse(translator, argValueList);
  }

  private Expression implementRecurse(RexToLixTranslator translator,
      final List<Expression> argValueList) {
    if (argValueList.size() == 1) {
      return argValueList.get(0);
    } else {
      return Expressions.condition(
          translator.checkNull(argValueList.get(0)),
          argValueList.get(0),
          implementRecurse(translator, Util.skip(argValueList)));
    }
  }
}

// End CoalesceImplementor.java
