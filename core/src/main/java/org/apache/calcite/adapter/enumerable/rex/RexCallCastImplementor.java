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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;

import java.util.List;

/** Implementor for the SQL {@code CAST} operator. */
class RexCallCastImplementor extends RexCallAbstractImplementor {

  RexCallCastImplementor(NullPolicy nullPolicy) {
    super(nullPolicy, false);
  }

  @Override String getVariableName() {
    return "cast";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    assert call.getOperands().size() == 1;
    final RelDataType sourceType = call.getOperands().get(0).getType();
    // It's only possible for the result to be null if both expression
    // and target type are nullable. We assume that the caller did not
    // make a mistake. If expression looks nullable, caller WILL have
    // checked that expression is not null before calling us.
    final boolean nullable =
        translator.isNullable(call)
            && sourceType.isNullable()
                && !Primitive.is(argValueList.get(0).getType());
    final RelDataType targetType =
        translator.nullifyType(call.getType(), nullable);
    return translator.translateCast(sourceType,
        targetType,
        argValueList.get(0));
  }
}

// End RexCallCastImplementor.java
