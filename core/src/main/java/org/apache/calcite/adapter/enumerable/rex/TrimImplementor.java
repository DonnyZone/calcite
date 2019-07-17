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
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Arrays;
import java.util.List;

/** Implementor for the {@code TRIM} function. */
class TrimImplementor extends AbstractRexCallImplementor {

  TrimImplementor() {
    super(NullPolicy.STRICT, false);
  }

  @Override String getVariableName() {
    return "trim";
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    final boolean strict = !translator.getConformance().allowExtendedTrim();
    final Expression flag =
        Expressions.convert_(argValueList.get(0),
            SqlTrimFunction.Flag.class);
    final Expression both = Expressions.equal(flag,
        Expressions.constant(SqlTrimFunction.Flag.BOTH));
    final Expression leading = Expressions.equal(flag,
        Expressions.constant(SqlTrimFunction.Flag.LEADING));
    final Expression trailing = Expressions.equal(flag,
        Expressions.constant(SqlTrimFunction.Flag.TRAILING));
    final Expression left = Expressions.foldOr(Arrays.asList(both, leading));
    final Expression right = Expressions.foldOr(Arrays.asList(both, trailing));
    return Expressions.call(
        BuiltInMethod.TRIM.method,
        left,
        right,
        argValueList.get(1),
        argValueList.get(2),
        Expressions.constant(strict));
  }
}

// End TrimImplementor.java
