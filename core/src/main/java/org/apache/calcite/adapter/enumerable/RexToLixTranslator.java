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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.rex.RexCallImplementor;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
  public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
      Util.mapOf(
          findMethod(String.class, "toUpperCase"), UPPER,
          findMethod(
              SqlFunctions.class, "substring", String.class, Integer.TYPE,
              Integer.TYPE), SUBSTRING,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHARACTER_LENGTH,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHAR_LENGTH,
          findMethod(SqlFunctions.class, "translate3", String.class, String.class,
              String.class), TRANSLATE3);

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final RexProgram program;
  private final SqlConformance conformance;
  private final Expression root;
  private final RexToLixTranslator.InputGetter inputGetter;
  private final BlockBuilder list;
  private final Map<? extends RexNode, Boolean> exprNullableMap;
  private final RexToLixTranslator parent;
  private final Function1<String, InputGetter> correlates;

  /**
   * Map from RexLiteral's variable name to its literal, which is often a
   * ({@link org.apache.calcite.linq4j.tree.ConstantExpression}))
   * It is used in the some {@link RexCall}'s implementors, such as
   * {@link org.apache.calcite.adapter.enumerable.rex.ExtractImplementor}
   */
  private final Map<Expression, Expression> literalMap = new HashMap<>();

  /**
   * For {@link RexCall}, keep the list of its operand's {@link Result}.
   * It is useful when creating a {@link CallImplementor},
   * see {@link RexImpTable#createImplementor(NotNullImplementor, NullPolicy, boolean)}
   */
  private final Map<RexCall, List<Result>> callOperandResultMap = new HashMap<>();

  /**
   * Map from RexNode under specific storage type to its Result,
   * to avoid generating duplicate code.
   * For {@link RexInputRef}, {@link RexDynamicParam} and {@link RexFieldAccess}
   */
  private final Map<Pair<RexNode, Type>, Result> rexWithStorageTypeResultMap = new HashMap<>();

  /**
   * Map from RexNode to its Result, to avoid generating duplicate code
   * For {@link RexLiteral} and {@link RexCall}
   */
  private final Map<RexNode, Result> rexResultMap = new HashMap<>();

  private Type currentStorageType = null;

  private static Method findMethod(
      Class<?> clazz, String name, Class... parameterTypes) {
    try {
      return clazz.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private RexToLixTranslator(RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      InputGetter inputGetter,
      BlockBuilder list,
      Map<? extends RexNode, Boolean> exprNullableMap,
      RexBuilder builder,
      SqlConformance conformance,
      RexToLixTranslator parent,
      Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.root = Objects.requireNonNull(root);
    this.inputGetter = inputGetter;
    this.list = Objects.requireNonNull(list);
    this.exprNullableMap = Objects.requireNonNull(exprNullableMap);
    this.builder = Objects.requireNonNull(builder);
    this.parent = parent; // may be null
    this.correlates = correlates; // may be null
  }

  @Override public Result visitInputRef(RexInputRef inputRef) {
    final Pair<RexNode, Type> key = Pair.of(inputRef, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final Expression valueExpression = inputGetter.field(
        list, inputRef.getIndex(), currentStorageType);
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(), list.newName("input_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLocalRef(RexLocalRef localRef) {
    return deref(localRef).accept(this);
  }

  @Override public Result visitLiteral(RexLiteral literal) {
    if (rexResultMap.containsKey(literal)) {
      return rexResultMap.get(literal);
    }
    final Type javaClass = typeFactory.getJavaClass(literal.getType());
    final Expression valueExpression;
    final Expression isNullExpression;
    if (literal.isNull()) {
      valueExpression = javaClass == null
          ? RexImpTable.NULL_EXPR : Expressions.constant(null, javaClass);
      isNullExpression = RexImpTable.TRUE_EXPR;
    } else {
      valueExpression =
          EnumUtils.getNotNullLiteralValueExpression(literal, javaClass);
      isNullExpression = RexImpTable.FALSE_EXPR;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(),
            list.newName("literal_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = Expressions.parameter(
        Boolean.TYPE, list.newName("literal_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    literalMap.put(valueVariable, valueExpression);
    final Result result = new Result(isNullVariable, valueVariable);
    rexResultMap.put(literal, result);
    return result;
  }

  @Override public Result visitCall(RexCall call) {
    if (rexResultMap.containsKey(call)) {
      return rexResultMap.get(call);
    }
    final SqlOperator operator = call.getOperator();
    final RexCallImplementor implementor =
        RexImpTable.INSTANCE.getRexCallImplementor(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    final List<RexNode> operandList = call.getOperands();
    final List<Type> storageTypes = EnumUtils.internalTypes(operandList);
    final List<Result> operandResults = new ArrayList<>();
    final Type originalStorageType = currentStorageType;
    for (int i = 0; i < operandList.size(); i++) {
      Type storageType = storageTypes.get(i);
      currentStorageType = storageType;
      Result operandResult = operandList.get(i).accept(this);
      if (storageType != null) {
        operandResult = toInnerStorageType(operandResult, storageType);
      }
      operandResults.add(operandResult);
    }
    callOperandResultMap.put(call, operandResults);
    currentStorageType = originalStorageType;
    final Result result = implementor.implement(this, call, operandResults);
    rexResultMap.put(call, result);
    return result;
  }

  private Result toInnerStorageType(final Result result, final Type storageType) {
    final Expression valueExpression =
        EnumUtils.enforce(storageType, result.valueVariable);
    if (valueExpression.equals(result.valueVariable)) {
      return result;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = result.isNullVariable;
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
    final Pair<RexNode, Type> key = Pair.of(dynamicParam, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final Type storageType = currentStorageType != null
        ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());
    final Expression valueExpression = EnumUtils.doConvert(
        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("?" + dynamicParam.getIndex())),
            storageType);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(), list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valueVariable)));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return result;
  }

  @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
    final Pair<RexNode, Type> key = Pair.of(fieldAccess, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final RexNode target = deref(fieldAccess.getReferenceExpr());
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
    case CORREL_VARIABLE:
      if (correlates == null) {
        throw new RuntimeException("Cannot translate " + fieldAccess
            + " since correlate variables resolver is not defined");
      }
      final RexToLixTranslator.InputGetter getter =
          correlates.apply(((RexCorrelVariable) target).getName());
      final Expression input = getter.field(
          list, fieldIndex, currentStorageType);
      final Expression condition = checkNull(input);
      final ParameterExpression valueVariable =
          Expressions.parameter(input.getType(), list.newName("corInp_value"));
      list.add(Expressions.declare(Modifier.FINAL, valueVariable, input));
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
      final Expression isNullExpression = Expressions.condition(
          condition,
          RexImpTable.TRUE_EXPR,
          checkNull(valueVariable));
      list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      final Result result1 = new Result(isNullVariable, valueVariable);
      rexWithStorageTypeResultMap.put(key, result1);
      return result1;
    default:
      RexNode rxIndex =
          builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
      RexNode rxName =
          builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
      RexCall accessCall = (RexCall) builder.makeCall(
          fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
          ImmutableList.of(target, rxIndex, rxName));
      final Result result2 = accessCall.accept(this);
      rexWithStorageTypeResultMap.put(key, result2);
      return result2;
    }
  }

  @Override public Result visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable
        + ". Correlated variables should always be referenced by field access");
  }

  @Override public Result visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public Result visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  /**
   * Translates a {@link RexProgram} to a sequence of expressions and
   * declarations.
   *
   * @param program Program to be translated
   * @param typeFactory Type factory
   * @param conformance SQL conformance
   * @param list List of statements, populated with declarations
   * @param outputPhysType Output type, or null
   * @param root Root expression
   * @param inputGetter Generates expressions for inputs
   * @param correlates Provider of references to the values of correlated
   *                   variables
   * @return Sequence of expressions, optional condition
   */
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, PhysType outputPhysType, Expression root,
      InputGetter inputGetter, Function1<String, InputGetter> correlates) {
    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
        null, null)
        .setCorrelates(correlates)
        .translateList(program.getProjectList(), storageTypes);
  }

  /** Creates a translator for translating aggregate functions. */
  public static RexToLixTranslator forAggregation(JavaTypeFactory typeFactory,
      BlockBuilder list, InputGetter inputGetter, SqlConformance conformance) {
    final ParameterExpression root = DataContext.ROOT;
    return new RexToLixTranslator(null, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), new RexBuilder(typeFactory), conformance, null,
        null);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  private Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    currentStorageType = storageType;
    final Result result = expr.accept(this);
    final Pair<ParameterExpression, Expression> handledResult = wrapResult(result);
    list.add(Expressions.declare(Modifier.FINAL, handledResult.left, handledResult.right));
    final Expression translated =
        EnumUtils.enforce(storageType, handledResult.left);
    assert translated != null;
    // When we asked for not null input that would be stored as box, avoid unboxing
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
        && translated.type.equals(storageType)) {
      return translated;
    }
    return nullAs.handle(translated);
  }

  /** Wrap Result as a single {@code Expression} with a boxed type
   * to avoid losing null semantic. */
  Pair<ParameterExpression, Expression> wrapResult(final Result result) {
    final Type returnType =
        Primitive.box(result.valueVariable.getType());
    final ParameterExpression resultVariable =
        Expressions.parameter(Modifier.FINAL, returnType, list.newName("result"));
    final Expression resultExpression = Expressions.condition(
            result.isNullVariable,
            RexImpTable.getDefaultValue(returnType),
            RexToLixTranslator.convert(result.valueVariable, returnType));
    return Pair.of(resultVariable, resultExpression);
  }

  /**
   * Handle checked Exceptions declared in Method. In such case,
   * method call should be wrapped in a try...catch block.
   * "
   *      final Type method_call;
   *      try {
   *        method_call = callExpr
   *      } catch (Exception e) {
   *        throw new RuntimeException(e);
   *      }
   * "
   */
  Expression handleMethodCheckedExceptions(Expression callExpr) {
    // Try statement
    ParameterExpression methodCall = Expressions.parameter(
        callExpr.getType(), list.newName("method_call"));
    list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
    Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
    // Catch Block, wrap checked exception in unchecked exception
    ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
    Expression uncheckedException = Expressions.new_(RuntimeException.class, e);
    CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedException));
    list.add(Expressions.tryCatch(st, cb));
    return methodCall;
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  private RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a literal.
   *
   * @throws AlwaysNull if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw AlwaysNull.INSTANCE;
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    return EnumUtils.getNotNullLiteralValueExpression(
        literal, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
        EnumUtils.internalTypes(operandList));
  }

  private List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, EnumUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  private List<Expression> translateList(List<? extends RexNode> operandList,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance) {
    if (program.getCondition() == null) {
      return RexImpTable.TRUE_EXPR;
    }
    final ParameterExpression root = DataContext.ROOT;
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
            null, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        program.getCondition(),
        RexImpTable.NullAs.FALSE);
  }

  public static Expression convert(Expression operand, Type toType) {
    final Type fromType = operand.getType();
    return convert(operand, fromType, toType);
  }

  public static Expression convert(Expression operand, Type fromType,
      Type toType) {
    return EnumUtils.doConvert(operand, fromType, toType);
  }

  /** Returns whether an expression is nullable. Even if its type says it is
   * nullable, if we have previously generated a check to make sure that it is
   * not null, we will say so.
   *
   * <p>For example, {@code WHERE a == b} translates to
   * {@code a != null && b != null && a.equals(b)}. When translating the
   * 3rd part of the disjunction, we already know a and b are not null.</p>
   *
   * @param e Expression
   * @return Whether expression is nullable in the current translation context
   */
  public boolean isNullable(RexNode e) {
    if (!e.getType().isNullable()) {
      return false;
    }
    final Boolean b = isKnownNullable(e);
    return b == null || b;
  }

  /**
   * Walks parent translator chain and verifies if the expression is nullable.
   *
   * @param node RexNode to check if it is nullable or not
   * @return null when nullability is not known, true or false otherwise
   */
  private Boolean isKnownNullable(RexNode node) {
    if (!exprNullableMap.isEmpty()) {
      Boolean nullable = exprNullableMap.get(node);
      if (nullable != null) {
        return nullable;
      }
    }
    return parent == null ? null : parent.isKnownNullable(node);
  }

  /** Creates a read-only copy of this translator that records that a given
   * expression is nullable. */
  public RexToLixTranslator setNullable(
      Map<? extends RexNode, Boolean> nullable) {
    if (nullable == null || nullable.isEmpty()) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        nullable, builder, conformance, this, correlates);
  }

  public RexToLixTranslator setBlock(BlockBuilder block) {
    if (block == list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        block, ImmutableMap.of(), builder, conformance, this, correlates);
  }

  RexToLixTranslator setCorrelates(
      Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), builder, conformance, this, correlates);
  }

  public Expression getRoot() {
    return root;
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private List<Pair<Expression, PhysType>> inputs;
    // A Map is necessary here to ensure the "current" method will
    // be called only once in code generated, because some functions
    // calculate ordinal based on its call times. See
    // {@link JdbcTest#testUnnestArrayScalarArrayWithOrdinal()}
    private Map<Expression, Expression> inputMap = new HashMap<>();

    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this.inputs = inputs;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      int offset = 0;
      for (Pair<Expression, PhysType> input : inputs) {
        final PhysType physType = input.right;
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left;
        if (!inputMap.containsKey(input.left)) {
          left = list.append("current", input.left);
          inputMap.put(input.left, left);
        } else {
          left = inputMap.get(input.left);
        }
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Thrown in the unusual (but not erroneous) situation where the expression
   * we are translating is the null literal but we have already checked that
   * it is not null. It is easier to throw (and caller will always handle)
   * than to check exhaustively beforehand. */
  static class AlwaysNull extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final AlwaysNull INSTANCE = new AlwaysNull();

    private AlwaysNull() {}
  }

  /**
   * Result of translating a {@code RexNode}
   */
  public static class Result {
    public final ParameterExpression isNullVariable;
    public final ParameterExpression valueVariable;

    public Result(ParameterExpression isNullVariable,
        ParameterExpression valueVariable) {
      this.isNullVariable = isNullVariable;
      this.valueVariable = valueVariable;
    }
  }

  public Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.FALSE_EXPR;
    }
    return Expressions.equal(expr, RexImpTable.NULL_EXPR);
  }
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public BlockBuilder getBlockBuilder() {
    return list;
  }

  public Expression getLiteral(Expression literalVariable) {
    return literalMap.get(literalVariable);
  }

  List<Result> getCallOperandResult(RexCall call) {
    return callOperandResultMap.get(call);
  }

  public SqlConformance getConformance() {
    return conformance;
  }

  public RexBuilder getRexBuilder() {
    return builder;
  }
}

// End RexToLixTranslator.java
