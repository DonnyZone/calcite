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
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.util.List;

class RexCallExtractImplementor extends RexCallAbstractImplementor {

  RexCallExtractImplementor(NullPolicy nullPolicy) {
    super(nullPolicy);
  }

  @Override String getVariableName() {
    return "extract";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    final TimeUnitRange timeUnitRange =
        (TimeUnitRange) ((ConstantExpression) argValueList.get(0)).value;
    final TimeUnit unit = timeUnitRange.startUnit;
    Expression operand = argValueList.get(1);
    final SqlTypeName sqlTypeName =
        call.operands.get(1).getType().getSqlTypeName();
    switch (unit) {
      case MILLENNIUM:
      case CENTURY:
      case YEAR:
      case QUARTER:
      case MONTH:
      case DAY:
      case DOW:
      case DECADE:
      case DOY:
      case ISODOW:
      case ISOYEAR:
      case WEEK:
        switch (sqlTypeName) {
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
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
            break;
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            operand = Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
            // fall through
          case TIMESTAMP:
            operand = Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                    operand, Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
            // fall through
          case DATE:
            return Expressions.call(BuiltInMethod.UNIX_DATE_EXTRACT.method,
                argValueList.get(0), operand);
          default:
            throw new AssertionError("unexpected " + sqlTypeName);
        }
        break;
      case MILLISECOND:
        return mod(operand, TimeUnit.MINUTE.multiplier.longValue());
      case MICROSECOND:
        operand = mod(operand, TimeUnit.MINUTE.multiplier.longValue());
        return Expressions.multiply(
            operand, Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
      case EPOCH:
        switch (sqlTypeName) {
          case DATE:
            // convert to milliseconds
            operand = Expressions.multiply(operand,
                Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
            // fall through
          case TIMESTAMP:
            // convert to seconds
            return Expressions.divide(operand,
                Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            operand = Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
            return Expressions.divide(operand,
                Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
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
            // no convertlet conversion, pass it as extract
            throw new AssertionError("unexpected " + sqlTypeName);
        }
        break;
      case HOUR:
      case MINUTE:
      case SECOND:
        switch (sqlTypeName) {
          case DATE:
            return Expressions.multiply(operand, Expressions.constant(0L));
        }
        break;
    }

    operand = mod(operand, getFactor(unit));
    if (unit == TimeUnit.QUARTER) {
      operand = Expressions.subtract(operand, Expressions.constant(1L));
    }
    operand = Expressions.divide(operand,
            Expressions.constant(unit.multiplier.longValue()));
    if (unit == TimeUnit.QUARTER) {
      operand = Expressions.add(operand, Expressions.constant(1L));
    }
    return operand;
  }

  private Expression mod(Expression operand, long factor) {
    if (factor == 1L) {
      return operand;
    } else {
      return Expressions.call(BuiltInMethod.FLOOR_MOD.method,
          operand, Expressions.constant(factor));
    }
  }

  private long getFactor(TimeUnit unit) {
    switch (unit) {
      case DAY:
        return 1L;
      case HOUR:
        return TimeUnit.DAY.multiplier.longValue();
      case MINUTE:
        return TimeUnit.HOUR.multiplier.longValue();
      case SECOND:
        return TimeUnit.MINUTE.multiplier.longValue();
      case MILLISECOND:
        return TimeUnit.SECOND.multiplier.longValue();
      case MONTH:
        return TimeUnit.YEAR.multiplier.longValue();
      case QUARTER:
        return TimeUnit.YEAR.multiplier.longValue();
      case YEAR:
      case DECADE:
      case CENTURY:
      case MILLENNIUM:
        return 1L;
      default:
        throw Util.unexpected(unit);
    }
  }
}

// End RexCallExtractImplementor.java
