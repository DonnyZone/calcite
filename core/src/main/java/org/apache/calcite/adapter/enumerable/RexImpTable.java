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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ImplementableAggFunction;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction;
import org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ANY_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BIT_AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BIT_OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COLLECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DENSE_RANK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FIRST_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FUSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING_ID;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUP_ID;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_ARRAYAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_OBJECTAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LEAD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LISTAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTH_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTILE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RANK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REGR_COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW_NUMBER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SINGLE_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM0;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
  public static final ConstantExpression NULL_EXPR =
      Expressions.constant(null);
  public static final ConstantExpression FALSE_EXPR =
      Expressions.constant(false);
  public static final ConstantExpression TRUE_EXPR =
      Expressions.constant(true);
  public static final ConstantExpression COMMA_EXPR =
      Expressions.constant(",");

  private final Map<SqlOperator, CallImplementor> map = new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends AggImplementor>> aggMap =
      new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends WinAggImplementor>> winAggMap =
      new HashMap<>();

  RexImpTable() {
    aggMap.put(JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
        JsonObjectAggImplementor
            .supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method));
    aggMap.put(JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
        JsonObjectAggImplementor
            .supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method));
    aggMap.put(JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
        JsonArrayAggImplementor
            .supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method));
    aggMap.put(JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
        JsonArrayAggImplementor
            .supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method));
    aggMap.put(COUNT, constructorSupplier(CountImplementor.class));
    aggMap.put(REGR_COUNT, constructorSupplier(CountImplementor.class));
    aggMap.put(SUM0, constructorSupplier(SumImplementor.class));
    aggMap.put(SUM, constructorSupplier(SumImplementor.class));
    Supplier<MinMaxImplementor> minMax =
        constructorSupplier(MinMaxImplementor.class);
    aggMap.put(MIN, minMax);
    aggMap.put(MAX, minMax);
    aggMap.put(ANY_VALUE, minMax);
    final Supplier<BitOpImplementor> bitop = constructorSupplier(BitOpImplementor.class);
    aggMap.put(BIT_AND, bitop);
    aggMap.put(BIT_OR, bitop);
    aggMap.put(SINGLE_VALUE, constructorSupplier(SingleValueImplementor.class));
    aggMap.put(COLLECT, constructorSupplier(CollectImplementor.class));
    aggMap.put(LISTAGG, constructorSupplier(ListaggImplementor.class));
    aggMap.put(FUSION, constructorSupplier(FusionImplementor.class));
    final Supplier<GroupingImplementor> grouping =
        constructorSupplier(GroupingImplementor.class);
    aggMap.put(GROUPING, grouping);
    aggMap.put(GROUP_ID, grouping);
    aggMap.put(GROUPING_ID, grouping);
    winAggMap.put(RANK, constructorSupplier(RankImplementor.class));
    winAggMap.put(DENSE_RANK, constructorSupplier(DenseRankImplementor.class));
    winAggMap.put(ROW_NUMBER, constructorSupplier(RowNumberImplementor.class));
    winAggMap.put(FIRST_VALUE,
        constructorSupplier(FirstValueImplementor.class));
    winAggMap.put(NTH_VALUE, constructorSupplier(NthValueImplementor.class));
    winAggMap.put(LAST_VALUE, constructorSupplier(LastValueImplementor.class));
    winAggMap.put(LEAD, constructorSupplier(LeadImplementor.class));
    winAggMap.put(LAG, constructorSupplier(LagImplementor.class));
    winAggMap.put(NTILE, constructorSupplier(NtileImplementor.class));
    winAggMap.put(COUNT, constructorSupplier(CountWinImplementor.class));
    winAggMap.put(REGR_COUNT, constructorSupplier(CountWinImplementor.class));
  }

  private <T> Supplier<T> constructorSupplier(Class<T> klass) {
    final Constructor<T> constructor;
    try {
      constructor = klass.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          klass + " should implement zero arguments constructor");
    }
    return () -> {
      try {
        return constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalStateException(
            "Error while creating aggregate implementor " + constructor, e);
      }
    };
  }

  public static CallImplementor createImplementor(
      final NotNullImplementor implementor,
      final NullPolicy nullPolicy,
      final boolean harmonize) {
    return null;
  }

  public static CallImplementor createImplementor(
      final Method method,
      final NullPolicy nullPolicy,
      final boolean harmonize) {

    return null;
  }

  public static final RexImpTable INSTANCE = new RexImpTable();

  public CallImplementor get(final SqlOperator operator) {
    if (operator instanceof SqlUserDefinedFunction) {
      org.apache.calcite.schema.Function udf =
          ((SqlUserDefinedFunction) operator).getFunction();
      if (!(udf instanceof ImplementableFunction)) {
        throw new IllegalStateException("User defined function " + operator
            + " must implement ImplementableFunction");
      }
      return ((ImplementableFunction) udf).getImplementor();
    }
    return map.get(operator);
  }

  public AggImplementor get(final SqlAggFunction aggregation,
      boolean forWindowAggregate) {
    if (aggregation instanceof SqlUserDefinedAggFunction) {
      final SqlUserDefinedAggFunction udaf =
          (SqlUserDefinedAggFunction) aggregation;
      if (!(udaf.function instanceof ImplementableAggFunction)) {
        throw new IllegalStateException("User defined aggregation "
            + aggregation + " must implement ImplementableAggFunction");
      }
      return ((ImplementableAggFunction) udaf.function)
          .getImplementor(forWindowAggregate);
    }
    if (forWindowAggregate) {
      Supplier<? extends WinAggImplementor> winAgg =
          winAggMap.get(aggregation);
      if (winAgg != null) {
        return winAgg.get();
      }
      // Regular aggregates can be used in window context as well
    }

    Supplier<? extends AggImplementor> aggSupplier = aggMap.get(aggregation);
    if (aggSupplier == null) {
      return null;
    }

    return aggSupplier.get();
  }

  /** Strategy what an operator should return if one of its
   * arguments is null. */
  public enum NullAs {
    /** The most common policy among the SQL built-in operators. If
     * one of the arguments is null, returns null. */
    NULL,

    /** If one of the arguments is null, the function returns
     * false. Example: {@code IS NOT NULL}. */
    FALSE,

    /** If one of the arguments is null, the function returns
     * true. Example: {@code IS NULL}. */
    TRUE,

    /** It is not possible for any of the arguments to be null.  If
     * the argument type is nullable, the enclosing code will already
     * have performed a not-null check. This may allow the operator
     * implementor to generate a more efficient implementation, for
     * example, by avoiding boxing or unboxing. */
    NOT_POSSIBLE,

    /** Return false if result is not null, true if result is null. */
    IS_NULL,

    /** Return true if result is not null, false if result is null. */
    IS_NOT_NULL;

    public static NullAs of(boolean nullable) {
      return nullable ? NULL : NOT_POSSIBLE;
    }

    /** Adapts an expression with "normal" result to one that adheres to
     * this particular policy. */
    public Expression handle(Expression x) {
      switch (Primitive.flavor(x.getType())) {
      case PRIMITIVE:
        // Expression cannot be null. We can skip any runtime checks.
        switch (this) {
        case NULL:
        case NOT_POSSIBLE:
        case FALSE:
        case TRUE:
          return x;
        case IS_NULL:
          return FALSE_EXPR;
        case IS_NOT_NULL:
          return TRUE_EXPR;
        default:
          throw new AssertionError();
        }
      case BOX:
        switch (this) {
        case NOT_POSSIBLE:
          return RexToLixTranslator.convert(
              x,
              Primitive.ofBox(x.getType()).primitiveClass);
        }
        // fall through
      }
      switch (this) {
      case NULL:
      case NOT_POSSIBLE:
        return x;
      case FALSE:
        return Expressions.call(
            BuiltInMethod.IS_TRUE.method,
            x);
      case TRUE:
        return Expressions.call(
            BuiltInMethod.IS_NOT_FALSE.method,
            x);
      case IS_NULL:
        return Expressions.equal(x, NULL_EXPR);
      case IS_NOT_NULL:
        return Expressions.notEqual(x, NULL_EXPR);
      default:
        throw new AssertionError();
      }
    }
  }

  static Expression getDefaultValue(Type type) {
    if (Primitive.is(type)) {
      Primitive p = Primitive.of(type);
      return Expressions.constant(p.defaultValue, type);
    }
    return Expressions.constant(null, type);
  }

  /** Multiplies an expression by a constant and divides by another constant,
   * optimizing appropriately.
   *
   * <p>For example, {@code multiplyDivide(e, 10, 1000)} returns
   * {@code e / 100}. */
  public static Expression multiplyDivide(Expression e, BigDecimal multiplier,
      BigDecimal divider) {
    if (multiplier.equals(BigDecimal.ONE)) {
      if (divider.equals(BigDecimal.ONE)) {
        return e;
      }
      return Expressions.divide(e,
          Expressions.constant(divider.intValueExact()));
    }
    final BigDecimal x =
        multiplier.divide(divider, RoundingMode.UNNECESSARY);
    switch (x.compareTo(BigDecimal.ONE)) {
    case 0:
      return e;
    case 1:
      return Expressions.multiply(e, Expressions.constant(x.intValueExact()));
    case -1:
      return multiplyDivide(e, BigDecimal.ONE, x);
    default:
      throw new AssertionError();
    }
  }

  /** Implementor for the {@code COUNT} aggregate function. */
  static class CountImplementor extends StrictAggImplementor {
    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      add.currentBlock().add(
          Expressions.statement(
              Expressions.postIncrementAssign(add.accumulator().get(0))));
    }
  }

  /** Implementor for the {@code COUNT} windowed aggregate function. */
  static class CountWinImplementor extends StrictWinAggImplementor {
    boolean justFrameRowCount;

    @Override public List<Type> getNotNullState(WinAggContext info) {
      boolean hasNullable = false;
      for (RelDataType type : info.parameterRelTypes()) {
        if (type.isNullable()) {
          hasNullable = true;
          break;
        }
      }
      if (!hasNullable) {
        justFrameRowCount = true;
        return Collections.emptyList();
      }
      return super.getNotNullState(info);
    }

    @Override public void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      if (justFrameRowCount) {
        return;
      }
      add.currentBlock().add(
          Expressions.statement(
              Expressions.postIncrementAssign(add.accumulator().get(0))));
    }

    @Override protected Expression implementNotNullResult(WinAggContext info,
        WinAggResultContext result) {
      if (justFrameRowCount) {
        return result.getFrameRowCount();
      }
      return super.implementNotNullResult(info, result);
    }
  }

  /** Implementor for the {@code SUM} windowed aggregate function. */
  static class SumImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Expression start = info.returnType() == BigDecimal.class
          ? Expressions.constant(BigDecimal.ZERO)
          : Expressions.constant(0);

      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), start)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression next;
      if (info.returnType() == BigDecimal.class) {
        next = Expressions.call(acc, "add", add.arguments().get(0));
      } else {
        next = Expressions.add(acc,
            Types.castIfNecessary(acc.type, add.arguments().get(0)));
      }
      accAdvance(add, acc, next);
    }

    @Override public Expression implementNotNullResult(AggContext info,
        AggResultContext result) {
      return super.implementNotNullResult(info, result);
    }
  }

  /** Implementor for the {@code MIN} and {@code MAX} aggregate functions. */
  static class MinMaxImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Expression acc = reset.accumulator().get(0);
      Primitive p = Primitive.of(acc.getType());
      boolean isMin = MIN == info.aggregation();
      Object inf = p == null ? null : (isMin ? p.max : p.min);
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc,
                  Expressions.constant(inf, acc.getType()))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression arg = add.arguments().get(0);
      SqlAggFunction aggregation = info.aggregation();
      final Method method = (aggregation == MIN
          ? BuiltInMethod.LESSER
          : BuiltInMethod.GREATER).method;
      Expression next = Expressions.call(
          method.getDeclaringClass(),
          method.getName(),
          acc,
          Expressions.unbox(arg));
      accAdvance(add, acc, next);
    }
  }

  /** Implementor for the {@code SINGLE_VALUE} aggregate function. */
  static class SingleValueImplementor implements AggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Arrays.asList(boolean.class, info.returnType());
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      List<Expression> acc = reset.accumulator();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0), Expressions.constant(false))));
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(1),
                  getDefaultValue(acc.get(1).getType()))));
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      List<Expression> acc = add.accumulator();
      Expression flag = acc.get(0);
      add.currentBlock().add(
          Expressions.ifThen(flag,
              Expressions.throw_(
                  Expressions.new_(IllegalStateException.class,
                      Expressions.constant("more than one value in agg "
                          + info.aggregation())))));
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(flag, Expressions.constant(true))));
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(1), add.arguments().get(0))));
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      return RexToLixTranslator.convert(result.accumulator().get(1),
          info.returnType());
    }
  }

  /** Implementor for the {@code COLLECT} aggregate function. */
  static class CollectImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      // acc[0] = new ArrayList();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      // acc[0].add(arg);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_ADD.method,
                  add.arguments().get(0))));
    }
  }

  /** Implementor for the {@code LISTAGG} aggregate function. */
  static class ListaggImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), NULL_EXPR)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      final Expression accValue = add.accumulator().get(0);
      final Expression arg0 = add.arguments().get(0);
      final Expression arg1 = add.arguments().size() == 2
          ? add.arguments().get(1) : COMMA_EXPR;
      final Expression result = Expressions.condition(
          Expressions.equal(NULL_EXPR, accValue),
          arg0,
          Expressions.call(BuiltInMethod.STRING_CONCAT.method, accValue,
              Expressions.call(BuiltInMethod.STRING_CONCAT.method, arg1, arg0)));

      add.currentBlock().add(Expressions.statement(Expressions.assign(accValue, result)));
    }
  }

  /** Implementor for the {@code FUSION} aggregate function. */
  static class FusionImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      // acc[0] = new ArrayList();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      // acc[0].add(arg);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_ADDALL.method,
                  add.arguments().get(0))));
    }
  }

  /** Implementor for the {@code BIT_AND} and {@code BIT_OR} aggregate function. */
  static class BitOpImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Object initValue = info.aggregation() == BIT_AND ? -1 : 0;
      Expression start = Expressions.constant(initValue, info.returnType());

      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), start)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression arg = add.arguments().get(0);
      SqlAggFunction aggregation = info.aggregation();
      final Method method = (aggregation == BIT_AND
          ? BuiltInMethod.BIT_AND
          : BuiltInMethod.BIT_OR).method;
      Expression next = Expressions.call(
          method.getDeclaringClass(),
          method.getName(),
          acc,
          Expressions.unbox(arg));
      accAdvance(add, acc, next);
    }
  }

  /** Implementor for the {@code GROUPING} aggregate function. */
  static class GroupingImplementor implements AggImplementor {
    public List<Type> getStateType(AggContext info) {
      return ImmutableList.of();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
    }

    public void implementAdd(AggContext info, AggAddContext add) {
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      final List<Integer> keys;
      switch (info.aggregation().kind) {
      case GROUPING: // "GROUPING(e, ...)", also "GROUPING_ID(e, ...)"
        keys = result.call().getArgList();
        break;
      case GROUP_ID: // "GROUP_ID()"
        // We don't implement GROUP_ID properly. In most circumstances, it
        // returns 0, so we always return 0. Logged
        // [CALCITE-1824] GROUP_ID returns wrong result
        keys = ImmutableIntList.of();
        break;
      default:
        throw new AssertionError();
      }
      Expression e = null;
      if (info.groupSets().size() > 1) {
        final List<Integer> keyOrdinals = info.keyOrdinals();
        long x = 1L << (keys.size() - 1);
        for (int k : keys) {
          final int i = keyOrdinals.indexOf(k);
          assert i >= 0;
          final Expression e2 =
              Expressions.condition(result.keyField(keyOrdinals.size() + i),
                  Expressions.constant(x),
                  Expressions.constant(0L));
          if (e == null) {
            e = e2;
          } else {
            e = Expressions.add(e, e2);
          }
          x >>= 1;
        }
      }
      return e != null ? e : Expressions.constant(0, info.returnType());
    }
  }

  /** Implementor for user-defined aggregate functions. */
  public static class UserDefinedAggReflectiveImplementor
      extends StrictAggImplementor {
    private final AggregateFunctionImpl afi;

    public UserDefinedAggReflectiveImplementor(AggregateFunctionImpl afi) {
      this.afi = afi;
    }

    @Override public List<Type> getNotNullState(AggContext info) {
      if (afi.isStatic) {
        return Collections.singletonList(afi.accumulatorType);
      }
      return Arrays.asList(afi.accumulatorType, afi.declaringClass);
    }

    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      List<Expression> acc = reset.accumulator();
      if (!afi.isStatic) {
        reset.currentBlock().add(
            Expressions.statement(
                Expressions.assign(acc.get(1),
                    Expressions.new_(afi.declaringClass))));
      }
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0),
                  Expressions.call(afi.isStatic
                      ? null
                      : acc.get(1), afi.initMethod))));
    }

    @Override protected void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      List<Expression> acc = add.accumulator();
      List<Expression> aggArgs = add.arguments();
      List<Expression> args = new ArrayList<>(aggArgs.size() + 1);
      args.add(acc.get(0));
      args.addAll(aggArgs);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0),
                  Expressions.call(afi.isStatic ? null : acc.get(1), afi.addMethod,
                      args))));
    }

    @Override protected Expression implementNotNullResult(AggContext info,
        AggResultContext result) {
      List<Expression> acc = result.accumulator();
      return Expressions.call(
          afi.isStatic ? null : acc.get(1), afi.resultMethod, acc.get(0));
    }
  }

  /** Implementor for the {@code RANK} windowed aggregate function. */
  static class RankImplementor extends StrictWinAggImplementor {
    @Override protected void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      Expression acc = add.accumulator().get(0);
      // This is an example of the generated code
      if (false) {
        new Object() {
          int curentPosition; // position in for-win-agg-loop
          int startIndex;     // index of start of window
          Comparable[] rows;  // accessed via WinAggAddContext.compareRows
          {
            if (curentPosition > startIndex) {
              if (rows[curentPosition - 1].compareTo(rows[curentPosition])
                  > 0) {
                // update rank
              }
            }
          }
        };
      }
      BlockBuilder builder = add.nestBlock();
      add.currentBlock().add(
          Expressions.ifThen(
              Expressions.lessThan(
                  add.compareRows(
                      Expressions.subtract(add.currentPosition(),
                          Expressions.constant(1)),
                      add.currentPosition()),
                  Expressions.constant(0)),
              Expressions.statement(
                  Expressions.assign(acc, computeNewRank(acc, add)))));
      add.exitBlock();
      add.currentBlock().add(
          Expressions.ifThen(
              Expressions.greaterThan(add.currentPosition(),
                  add.startIndex()),
              builder.toBlock()));
    }

    protected Expression computeNewRank(Expression acc, WinAggAddContext add) {
      Expression pos = add.currentPosition();
      if (!add.startIndex().equals(Expressions.constant(0))) {
        // In general, currentPosition-startIndex should be used
        // However, rank/dense_rank does not allow preceding/following clause
        // so we always result in startIndex==0.
        pos = Expressions.subtract(pos, add.startIndex());
      }
      return pos;
    }

    @Override protected Expression implementNotNullResult(
        WinAggContext info, WinAggResultContext result) {
      // Rank is 1-based
      return Expressions.add(super.implementNotNullResult(info, result),
          Expressions.constant(1));
    }
  }

  /** Implementor for the {@code DENSE_RANK} windowed aggregate function. */
  static class DenseRankImplementor extends RankImplementor {
    @Override protected Expression computeNewRank(Expression acc,
        WinAggAddContext add) {
      return Expressions.add(acc, Expressions.constant(1));
    }
  }

  /** Implementor for the {@code FIRST_VALUE} and {@code LAST_VALUE}
   * windowed aggregate functions. */
  static class FirstLastValueImplementor implements WinAggImplementor {
    private final SeekType seekType;

    protected FirstLastValueImplementor(SeekType seekType) {
      this.seekType = seekType;
    }

    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return true;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      return Expressions.condition(winResult.hasRows(),
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), seekType))
              .translate(winResult.rexArguments().get(0), info.returnType()),
          getDefaultValue(info.returnType()));
    }
  }

  /** Implementor for the {@code FIRST_VALUE} windowed aggregate function. */
  static class FirstValueImplementor extends FirstLastValueImplementor {
    protected FirstValueImplementor() {
      super(SeekType.START);
    }
  }

  /** Implementor for the {@code LAST_VALUE} windowed aggregate function. */
  static class LastValueImplementor extends FirstLastValueImplementor {
    protected LastValueImplementor() {
      super(SeekType.END);
    }
  }

  /** Implementor for the {@code NTH_VALUE}
   * windowed aggregate function. */
  static class NthValueImplementor implements WinAggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return true;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      ParameterExpression res = Expressions.parameter(0, info.returnType(),
          result.currentBlock().newName("nth"));

      RexToLixTranslator currentRowTranslator =
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), SeekType.START));

      Expression dstIndex = winResult.computeIndex(
          Expressions.subtract(
              currentRowTranslator.translate(rexArgs.get(1), int.class),
              Expressions.constant(1)), SeekType.START);

      Expression rowInRange = winResult.rowInPartition(dstIndex);

      BlockBuilder thenBlock = result.nestBlock();
      Expression nthValue = winResult.rowTranslator(dstIndex)
          .translate(rexArgs.get(0), res.type);
      thenBlock.add(Expressions.statement(Expressions.assign(res, nthValue)));
      result.exitBlock();
      BlockStatement thenBranch = thenBlock.toBlock();

      Expression defaultValue = getDefaultValue(res.type);

      result.currentBlock().add(Expressions.declare(0, res, null));
      result.currentBlock().add(
          Expressions.ifThenElse(rowInRange, thenBranch,
              Expressions.statement(Expressions.assign(res, defaultValue))));
      return res;
    }
  }

  /** Implementor for the {@code LEAD} and {@code LAG} windowed
   * aggregate functions. */
  static class LeadLagImplementor implements WinAggImplementor {
    private final boolean isLead;

    protected LeadLagImplementor(boolean isLead) {
      this.isLead = isLead;
    }

    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return false;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      ParameterExpression res = Expressions.parameter(0, info.returnType(),
          result.currentBlock().newName(isLead ? "lead" : "lag"));

      Expression offset;
      RexToLixTranslator currentRowTranslator =
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), SeekType.SET));
      if (rexArgs.size() >= 2) {
        // lead(x, offset) or lead(x, offset, default)
        offset = currentRowTranslator.translate(
            rexArgs.get(1), int.class);
      } else {
        offset = Expressions.constant(1);
      }
      if (!isLead) {
        offset = Expressions.negate(offset);
      }
      Expression dstIndex = winResult.computeIndex(offset, SeekType.SET);

      Expression rowInRange = winResult.rowInPartition(dstIndex);

      BlockBuilder thenBlock = result.nestBlock();
      Expression lagResult = winResult.rowTranslator(dstIndex).translate(
          rexArgs.get(0), res.type);
      thenBlock.add(Expressions.statement(Expressions.assign(res, lagResult)));
      result.exitBlock();
      BlockStatement thenBranch = thenBlock.toBlock();

      Expression defaultValue = rexArgs.size() == 3
          ? currentRowTranslator.translate(rexArgs.get(2), res.type)
          : getDefaultValue(res.type);

      result.currentBlock().add(Expressions.declare(0, res, null));
      result.currentBlock().add(
          Expressions.ifThenElse(rowInRange, thenBranch,
              Expressions.statement(Expressions.assign(res, defaultValue))));
      return res;
    }
  }

  /** Implementor for the {@code LEAD} windowed aggregate function. */
  public static class LeadImplementor extends LeadLagImplementor {
    protected LeadImplementor() {
      super(true);
    }
  }

  /** Implementor for the {@code LAG} windowed aggregate function. */
  public static class LagImplementor extends LeadLagImplementor {
    protected LagImplementor() {
      super(false);
    }
  }

  /** Implementor for the {@code NTILE} windowed aggregate function. */
  static class NtileImplementor implements WinAggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return false;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      Expression tiles =
          winResult.rowTranslator(winResult.index()).translate(
              rexArgs.get(0), int.class);

      Expression ntile =
          Expressions.add(Expressions.constant(1),
              Expressions.divide(
                  Expressions.multiply(
                      tiles,
                      Expressions.subtract(
                          winResult.index(), winResult.startIndex())),
                  winResult.getPartitionRowCount()));

      return ntile;
    }
  }

  /** Implementor for the {@code ROW_NUMBER} windowed aggregate function. */
  static class RowNumberImplementor extends StrictWinAggImplementor {
    @Override public List<Type> getNotNullState(WinAggContext info) {
      return Collections.emptyList();
    }

    @Override protected void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      // no op
    }

    @Override protected Expression implementNotNullResult(
        WinAggContext info, WinAggResultContext result) {
      // Window cannot be empty since ROWS/RANGE is not possible for ROW_NUMBER
      return Expressions.add(
          Expressions.subtract(result.index(), result.startIndex()),
          Expressions.constant(1));
    }
  }

  /** Implementor for the {@code JSON_OBJECTAGG} aggregate function. */
  static class JsonObjectAggImplementor implements AggImplementor {
    private final Method m;

    JsonObjectAggImplementor(Method m) {
      this.m = m;
    }

    static Supplier<JsonObjectAggImplementor> supplierFor(Method m) {
      return () -> new JsonObjectAggImplementor(m);
    }

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.singletonList(Map.class);
    }

    @Override public void implementReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(HashMap.class))));
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      final SqlJsonObjectAggAggFunction function =
          (SqlJsonObjectAggAggFunction) info.aggregation();
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(m,
                  Iterables.concat(
                      Collections.singletonList(add.accumulator().get(0)),
                      add.arguments(),
                      Collections.singletonList(
                          Expressions.constant(function.getNullClause()))))));
    }

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      return Expressions.call(BuiltInMethod.JSONIZE.method,
          result.accumulator().get(0));
    }
  }

  /** Implementor for the {@code JSON_ARRAYAGG} aggregate function. */
  static class JsonArrayAggImplementor implements AggImplementor {
    private final Method m;

    JsonArrayAggImplementor(Method m) {
      this.m = m;
    }

    static Supplier<JsonArrayAggImplementor> supplierFor(Method m) {
      return () -> new JsonArrayAggImplementor(m);
    }

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.singletonList(List.class);
    }

    @Override public void implementReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementAdd(AggContext info,
        AggAddContext add) {
      final SqlJsonArrayAggAggFunction function =
          (SqlJsonArrayAggAggFunction) info.aggregation();
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(m,
                  Iterables.concat(
                      Collections.singletonList(add.accumulator().get(0)),
                      add.arguments(),
                      Collections.singletonList(
                          Expressions.constant(function.getNullClause()))))));
    }

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      return Expressions.call(BuiltInMethod.JSONIZE.method,
          result.accumulator().get(0));
    }
  }
}

// End RexImpTable.java
