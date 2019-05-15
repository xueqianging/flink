/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The {@code savepoint-connector} package provides powerful functionality for reading, writing and
 * modify {@link org.apache.flink.runtime.checkpoint.savepoint.Savepoint}'s using Flink's batch
 * {@link org.apache.flink.api.java.DataSet} api.
 *
 * <p>This can be useful for:
 *
 * <ul>
 *   <li>Analyzing state for interesting patterns.
 *   <li>Troubleshooting or auditing Flink jobs by checking for discrepancies in state.
 *   <li>Bootstrapping state for new applications.
 *   <li>Modifying savepoints such as:
 *       <ul>
 *         <li>Changing max parallelism
 *         <li>Changing your state backend
 *         <li>Making breaking schema changes
 *         <li>Correcting invalid state
 *       </ul>
 * </ul>
 *
 * To understand how best to interact with {@code Savepoint}'s in a batch context, it is important
 * to understand how the data in a Flink state relates to a traditional relational database.
 *
 * <p>A database can be thought of as one or more namespaces, each containing a collection of
 * tables. Those tables in turn contain columns whose values have some intrinsic relationship
 * between them, such as being scoped under the same key.
 *
 * <p>A {@code Savepoint} represents the state of a job, which contains many operators. Those
 * operators, contain various kinds of state, both partitioned or keyed state along with
 * non-partitioned or operator state.
 *
 * <p>Consider the following Flink job:
 *
 * <pre>{@code
 * MapStateDescriptor<Integer, Double> CURRENCY_RATES = new MapStateDescriptor("rates", Types.INT, Types.DOUBLE);
 *
 * class CurrencyConverter extends BroadcastProcessFunction<Transaction, CurrencyRate, Transaction> {
 *      public void processElement(Transaction value, ReadOnlyContext ctx, Collector<Transaction> out) throws Exception {
 *          Double rate = ctx.getBroadcastState(CURRENCY_RATES).get(value.currencyId);
 *
 *          if (rate != null) {
 *              value.amount *= rate;
 *          }
 *
 *          out.collect(value);
 *      }
 *
 *      public void processBroadcastElement(CurrencyRate value, Context ctx, Collector<Transaction> out) throws Exception {
 *          ctx.getBroadcastState(CURRENCY_RATES).put(value.currencyId, value.rate);
 *      }
 * }
 *
 * class Summarize extends RichFlatMapFunction<Transaction, Summary> {
 *      transient ValueState<Double> totalState;
 *      transient ValueState<Integer> countState;
 *
 *      public void open(Configuration configuration) throw Exception {
 *          totalState = getRuntimeContext.getState(new ValueStateDescriptor("total", Types.DOUBLE);
 *          countState = getRuntimeContext.getState(new ValueStateDescriptor("count", Types.INT);
 *      }
 *
 *      public void flatMap(Transaction value, Collector<Summary> out) throws Exception {
 *          Summary summary = new Summary();
 *          summary.total = value.amount;
 *          summary.count = 1;
 *
 *          Double currentTotal = totalState.value();
 *          if (currentTotal != null) {
 *              summary.total += currentTotal;
 *          }
 *
 *          Integer currentCount = totalState.value();
 *          if (currentCount != null) {
 *              summary.count += currentCount;
 *          }
 *
 *          totalState.update(summary.total);
 *          currentCount.update(summary.count);
 *      }
 * }
 *
 * DataStream<Transaction> transactions = . . .
 * BroadcastStream<CurrencyRate> rates = . . .
 *
 * transactions
 *      .connect(rates)
 *      .transform(new CurrencyConverter())
 *      .uid("currency_converter")
 *      .keyBy(transaction -> transaction.accountId)
 *      .flatMap(new Summarize())
 *      .uid("summarize")
 * }</pre>
 *
 * This job contains multiple operators along with various kinds of state. When analyzing that state
 * we can first scope data by its operator, named by setting the uid on the operator. Within each
 * operator we can then look at the registered states. {@code CurrencyConverter} has a {@link
 * org.apache.flink.api.common.state.BroadcastState}, which is a type of non-partitioned operator
 * state. In general, there is no relationship between any two elements in an operator state and so
 * we can look at each value as being its own row. Contrast this with {@code Summarize} which
 * contains two keyed states. Because both states are scoped under the same key we can safely assume
 * there exists some relationship between the two values. Therefore, keyed state is best understood
 * as a single table per operator containing one "key" column along with n value columns, one for
 * each registered state. All of this comes to mean that the state for this job could be described
 * using the following pseudo-sql commands.
 *
 * <pre>{@code
 * CREATE NAMESPACE currency_converter;
 *
 * CREATE TABLE currency_converter.rates (
 *     entry Map.Entry<Integer, Double>
 * );
 *
 * CREATE NAMESPACE summarize;
 *
 * CREATE TABLE summarize.keyed_state (
 *     key   INTEGER PRIMARY KEY,
 *     total DOUBLE,
 *     count INTEGER
 * );
 * }</pre>
 *
 * In general {@code Savepoint}-database relationship can be summarized as:
 *
 * <ul>
 *   <li>A savepoint is a database
 *   <li>An operator is a namespaces named using its uid
 *   <li>Each operator state represents a single table
 *       <ul>
 *         <li>Each element in an operator state represents a single row in that table
 *       </ul>
 *   <li>Each operator containing keyed state has a single "keyed_state" table
 *       <ul>
 *         <li>Each keyed_state table has one key column mapping the key value of the operator
 *         <li>Each registered state represents a single column in the table
 *         <li>Each row in the table maps to a single key
 *       </ul>
 * </ul>
 *
 * <pre>
 *  ------------------------------------------------------
 *  | database               | savepoint                 |
 *  |    - namespace1        |   - uid1                  |
 *  |        - tableA        |       - operator_state_1  |
 *  |            - column1   |           - value         |
 *  |            - column2   |       - operator_state_2  |
 *  |        - tableB        |           - value         |
 *  |    - namespace2        |       - keyed_state       |
 *  |        - tableA        |           - key           |
 *  |        - tableB        |           - column1       |
 *  |                        |           - column2       |
 *  ------------------------------------------------------
 * </pre>
 */
package org.apache.flink.connectors.savepoint;
