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

package org.apache.flink.quickstart.common.source;

import org.apache.flink.quickstart.common.entity.Transaction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class TransactionIterator implements Iterator<Transaction> {

	private int index = 0;

	@Override
	public boolean hasNext() {
		return index < data.size();
	}

	@Override
	public Transaction next() {
		return data.get(index++);
	}

	private static List<Transaction> data = Arrays.asList(
		new Transaction(1, 1560498112127L, 188.2357099003657),
		new Transaction(2, 1560480652222L, 374.79335836604326),
		new Transaction(3, 1560493478958L, 112.152118429468),
		new Transaction(4, 1560507019035L, 478.75442648504594),
		new Transaction(5, 1560481071291L, 208.85072866402163),
		new Transaction(1, 1560420731276L, 379.64495019436447),
		new Transaction(2, 1560497742040L, 351.4485232861889),
		new Transaction(3, 1560424318298L, 320.7570952945281),
		new Transaction(4, 1560469962491L, 259.4212754385255),
		new Transaction(5, 1560446725813L, 273.44700960921034),
		new Transaction(1, 1560503983769L, 267.25368763116916),
		new Transaction(2, 1560520134635L, 397.15079938845406),
		new Transaction(3, 1560480668523L, 0.2194066966005),
		new Transaction(4, 1560481886895L, 231.94604654621705),
		new Transaction(5, 1560463113131L, 384.7377229475877),
		new Transaction(1, 1560466797395L, 419.6217879456744),
		new Transaction(2, 1560514011991L, 412.91879994415115),
		new Transaction(3, 1560501035021L, 0.77554174099876),
		new Transaction(4, 1560486268269L, 22.10557146881511),
		new Transaction(5, 1560422763061L, 377.54288020407364),
		new Transaction(1, 1560442113581L, 375.44798031843044),
		new Transaction(2, 1560501372037L, 230.1832122421834),
		new Transaction(3, 1560498125914L, 0.80402310053829),
		new Transaction(4, 1560483677015L, 350.89485314025745),
		new Transaction(5, 1560468089208L, 127.55210442056027),
		new Transaction(1, 1560442833642L, 483.91312286584264),
		new Transaction(2, 1560455920655L, 228.2204924842225),
		new Transaction(3, 1560505453981L, 871.15712846560719),
		new Transaction(4, 1560445660134L, 64.19220746924974),
		new Transaction(5, 1560482058290L, 79.4340502416104),
		new Transaction(1, 1560426601800L, 56.12746295371846),
		new Transaction(2, 1560506986508L, 256.4841651512737),
		new Transaction(3, 1560511663170L, 148.16082011960077),
		new Transaction(4, 1560443631729L, 199.95550543395672),
		new Transaction(5, 1560491034082L, 252.3765027547406),
		new Transaction(1, 1560507027143L, 274.73396383251946),
		new Transaction(2, 1560456547961L, 473.5428611008993),
		new Transaction(3, 1560481784538L, 119.92282164735049),
		new Transaction(4, 1560463731930L, 323.5957606292019),
		new Transaction(5, 1560449242644L, 353.1650892230241),
		new Transaction(1, 1560499851928L, 211.90071205453455),
		new Transaction(2, 1560504649412L, 280.9327129913271),
		new Transaction(3, 1560431167819L, 347.89484651411004),
		new Transaction(4, 1560496244056L, 459.86861691191416),
		new Transaction(5, 1560471670987L, 82.31238068869244),
		new Transaction(1, 1560498062753L, 373.2607107024356),
		new Transaction(2, 1560420559219L, 479.83100373430494),
		new Transaction(3, 1560447685653L, 454.2514106070776),
		new Transaction(4, 1560481120095L, 83.64165866821773),
		new Transaction(5, 1560452308232L, 292.44188319396284));
}
