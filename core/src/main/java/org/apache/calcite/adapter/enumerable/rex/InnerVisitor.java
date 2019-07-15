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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InnerVisitor implements RexVisitor<RexNodeGenResult> {

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  private final RexToLixTranslator.InputGetter inputGetter;
  final BlockBuilder list;
  private Type storageType;
  private final Function1<String, RexToLixTranslator.InputGetter> correlates;

  public InnerVisitor(RexProgram program, JavaTypeFactory typeFactory,
                      Expression root, RexToLixTranslator.InputGetter inputGetter, RexBuilder builder,
                      SqlConformance conformance, Function1<String, RexToLixTranslator.InputGetter> correlates,
                      Type storageType) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.root = Objects.requireNonNull(root);
    this.inputGetter = inputGetter;
    this.list = new BlockBuilder(false);
    this.builder = Objects.requireNonNull(builder);
    this.correlates = correlates; // may be null
    this.storageType = storageType;
  }

  public Expression getRoot() {
    return root;
  }

  public BlockBuilder getCode() {
    return list;
  }

  public void setStorageType(Type storageType) {
    this.storageType = storageType;
  }

  @Override public RexNodeGenResult visitInputRef(RexInputRef inputRef) {
    Expression input = inputGetter.field(list, inputRef.getIndex(), storageType);
    ParameterExpression isNull = Expressions.parameter(Boolean.TYPE, list.newName("isNull_input"));
    ParameterExpression value = Expressions.parameter(input.getType(), list.newName("value_input"));
    list.add(Expressions.declare(Modifier.FINAL, isNull, checkNull(input)));
    list.add(Expressions.declare(Modifier.FINAL, value, input));
    return new RexNodeGenResult(isNull, value);
  }

  @Override public RexNodeGenResult visitLocalRef(RexLocalRef localRef) {
    final RexNode rexNode = deref(localRef);
    assert localRef.getType().equals(rexNode.getType());
    return rexNode.accept(this);
  }

  @Override public RexNodeGenResult visitLiteral(RexLiteral literal) {
    ParameterExpression isNull =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_literal"));
    Type javaClass = typeFactory.getJavaClass(literal.getType());
    ParameterExpression value =
        Expressions.parameter(javaClass, list.newName("value_literal"));
    if (literal.isNull()) {
      list.add(Expressions.declare(Modifier.FINAL, isNull, RexCallImpTable.TRUE_EXPR));
      list.add(Expressions.declare(Modifier.FINAL, value,
              RexToLixTranslator.convert(RexCallImpTable.NULL_EXPR, value.getType())));
    } else {
      list.add(Expressions.declare(Modifier.FINAL, isNull, RexCallImpTable.FALSE_EXPR));
      list.add(Expressions.declare(Modifier.FINAL, value,
              CodegenUtil.getLiteralValueExpression(literal, javaClass)));
    }
    return new RexNodeGenResult(isNull, value);
  }

  @Override public RexNodeGenResult visitCall(RexCall call) {
    final SqlOperator operator = call.getOperator();
    RexCallImplementor implementor = RexCallImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    List<RexNodeGenResult> arguments = new ArrayList<>();
    for (RexNode operand: call.getOperands()) {
      arguments.add(operand.accept(this));
    }
    return implementor.implement(this, call, arguments);
  }

  @Override public RexNodeGenResult visitDynamicParam(RexDynamicParam dynamicParam) {
    if (storageType == null) {
      storageType = typeFactory.getJavaClass(dynamicParam.getType());
    }
    Expression pe = CodegenUtil.convert(
            Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                    Expressions.constant("?" + dynamicParam.getIndex())),
            storageType);
    ParameterExpression isNull =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    ParameterExpression value =
        Expressions.parameter(pe.getType(), list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, isNull, checkNull(value)));
    list.add(Expressions.declare(Modifier.FINAL, value, value));
    return new RexNodeGenResult(isNull, value);
  }

  @Override public RexNodeGenResult visitFieldAccess(RexFieldAccess fieldAccess) {
    RexNode target = deref(fieldAccess.getReferenceExpr());;
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
      case CORREL_VARIABLE:
        if (correlates == null) {
          throw new RuntimeException("Cannot translate " + fieldAccess + " since "
                  + "correlate variables resolver is not defined");
        }
        RexToLixTranslator.InputGetter getter =
            correlates.apply(((RexCorrelVariable) target).getName());
        Expression input = getter.field(list, fieldIndex, storageType);
        ParameterExpression isNull =
            Expressions.parameter(Boolean.TYPE, list.newName("isNull_corInp"));
        ParameterExpression value =
            Expressions.parameter(input.getType(), list.newName("value_corInp"));
        list.add(Expressions.declare(Modifier.FINAL, isNull, checkNull(input)));
        list.add(Expressions.declare(Modifier.FINAL, value, input));
        return new RexNodeGenResult(isNull, value);
      default:
        RexNode rxIndex = builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
        RexNode rxName = builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
        RexCall accessCall = (RexCall) builder.makeCall(
                fieldAccess.getType(),
                SqlStdOperatorTable.STRUCT_ACCESS,
                ImmutableList.of(target, rxIndex, rxName));
        return accessCall.accept(this);
    }
  }

  @Override public RexNodeGenResult visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable + ". Correlated"
        + " variables should always be referenced by field access");
  }

  @Override public RexNodeGenResult visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public RexNodeGenResult visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public RexNodeGenResult visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override
  public RexNodeGenResult visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public RexNodeGenResult visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  /*--------------------------------helper methods-------------------------------*/

  private Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexCallImpTable.FALSE_EXPR;
    }
    return Expressions.notEqual(expr, RexCallImpTable.NULL_EXPR);
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  RelDataType nullifyType(RelDataType type, boolean nullable) {
    if (!nullable) {
      final Primitive primitive = javaPrimitive(type);
      if (primitive != null) {
        return typeFactory.createJavaType(primitive.primitiveClass);
      }
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  public boolean isNullable(RexNode e) {
    return e.getType().isNullable();
  }

  private Primitive javaPrimitive(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      return Primitive.ofBox(
          ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }
    return null;
  }

  Expression translateCast(RelDataType sourceType,
      RelDataType targetType, Expression operand) {
    return CodegenUtil.translateCast(sourceType,
        targetType, operand, root, typeFactory);
  }

}

// End InnerVisitor.java
