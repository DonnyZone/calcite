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
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

class RexCallDatetimeArithmeticImplementor extends RexCallAbstractImplementor {

  RexCallDatetimeArithmeticImplementor(NullPolicy nullPolicy) {
    super(nullPolicy, false);
  }

  @Override String getVariableName() {
    return "dateTime_arithmetic";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    final RexNode operand0 = call.getOperands().get(0);
    Expression trop0 = argValueList.get(0);
    final SqlTypeName typeName1 =
        call.getOperands().get(1).getType().getSqlTypeName();
    Expression trop1 = argValueList.get(1);
    final SqlTypeName typeName = call.getType().getSqlTypeName();
    switch (operand0.getType().getSqlTypeName()) {
      case DATE:
        switch (typeName) {
          case TIMESTAMP:
            trop0 = Expressions.convert_(
                Expressions.multiply(trop0,
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                long.class);
            break;
          default:
            switch (typeName1) {
              case INTERVAL_DAY:
              case INTERVAL_DAY_HOUR:
              case INTERVAL_DAY_MINUTE:
              case INTERVAL_DAY_SECOND:
              case INTERVAL_HOUR:
              case INTERVAL_HOUR_MINUTE:
              case INTERVAL_HOUR_SECOND:
              case INTERVAL_MINUTE:
              case INTERVAL_MINUTE_SECOND:
              case INTERVAL_SECOND:
                trop1 = Expressions.convert_(
                    Expressions.divide(trop1,
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                        int.class);
            }
        }
        break;
      case TIME:
        trop1 = Expressions.convert_(trop1, int.class);
        break;
    }
    switch (typeName1) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        switch (call.getKind()) {
          case MINUS:
            trop1 = Expressions.negate(trop1);
        }
        switch (typeName) {
          case TIME:
            return Expressions.convert_(trop0, long.class);
          default:
            final BuiltInMethod method =
                    operand0.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP
                        ? BuiltInMethod.ADD_MONTHS
                        : BuiltInMethod.ADD_MONTHS_INT;
            return Expressions.call(method.method, trop0, trop1);
        }

      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        switch (call.getKind()) {
          case MINUS:
            return normalize(typeName, Expressions.subtract(trop0, trop1));
          default:
            return normalize(typeName, Expressions.add(trop0, trop1));
        }
      default:
        switch (call.getKind()) {
          case MINUS:
            switch (typeName) {
              case INTERVAL_YEAR:
              case INTERVAL_YEAR_MONTH:
              case INTERVAL_MONTH:
                return Expressions.call(BuiltInMethod.SUBTRACT_MONTHS.method,
                        trop0, trop1);
            }
            TimeUnit fromUnit = typeName1 == SqlTypeName.DATE ? TimeUnit.DAY : TimeUnit.MILLISECOND;
            TimeUnit toUnit = TimeUnit.MILLISECOND;
            return CodegenUtil.multiplyDivide(
                    Expressions.convert_(Expressions.subtract(trop0, trop1),
                            (Class) long.class),
                    fromUnit.multiplier, toUnit.multiplier);
          default:
            throw new AssertionError(call);
        }
    }
  }

  /** Normalizes a TIME value into 00:00:00..23:59:39. */
  private Expression normalize(SqlTypeName typeName, Expression e) {
    switch (typeName) {
      case TIME:
        return Expressions.call(BuiltInMethod.FLOOR_MOD.method, e,
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
      default:
        return e;
    }
  }
}

// End RexCallDatetimeArithmeticImplementor.java
