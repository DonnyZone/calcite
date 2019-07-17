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
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

/** Implementor for the {@code FLOOR} and {@code CEIL} functions. */
class FloorImplementor extends MethodNameImplementor {

  private final Method timestampMethod;
  private final Method dateMethod;

  FloorImplementor(String methodName, Method timestampMethod,
      Method dateMethod) {
    super(methodName, NullPolicy.STRICT, false);
    this.timestampMethod = timestampMethod;
    this.dateMethod = dateMethod;
  }

  @Override String getVariableName() {
    return "floor";
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    switch (call.getOperands().size()) {
    case 1:
      switch (call.getType().getSqlTypeName()) {
      case BIGINT:
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        return argValueList.get(0);
      default:
        return super.implementSafe(translator, call, argValueList);
      }
    case 2:
      final Type type;
      final Method floorMethod;
      Expression operand = argValueList.get(0);
      switch (call.getType().getSqlTypeName()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        operand = Expressions.call(
            BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
            operand,
            Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
        // fall through
      case TIMESTAMP:
        type = long.class;
        floorMethod = timestampMethod;
        break;
      default:
        type = int.class;
        floorMethod = dateMethod;
      }
      final ConstantExpression tur =
          (ConstantExpression) translator.getLiteral(argValueList.get(1));
      final TimeUnitRange timeUnitRange = (TimeUnitRange) tur.value;
      switch (timeUnitRange) {
      case YEAR:
      case MONTH:
        return Expressions.call(floorMethod, tur,
            call(operand, type, TimeUnit.DAY));
      case NANOSECOND:
      default:
        return call(operand, type, timeUnitRange.startUnit);
      }
    default:
      throw new AssertionError();
    }
  }

  private Expression call(Expression operand, Type type,
      TimeUnit timeUnit) {
    return Expressions.call(SqlFunctions.class, methodName,
        Types.castIfNecessary(type, operand),
            Types.castIfNecessary(type,
                Expressions.constant(timeUnit.multiplier)));
  }
}

// End FloorImplementor.java
