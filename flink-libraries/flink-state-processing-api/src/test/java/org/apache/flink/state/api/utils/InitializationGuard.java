/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.utils;

import org.apache.flink.util.SerializableObject;

import javax.annotation.concurrent.GuardedBy;

import java.io.Serializable;

/**
 * A lock that protects a resource until it is initialized.
 * Resource initialization may or may not be successful.
 */
@SuppressWarnings("WeakerAccess")
public class InitializationGuard implements Serializable, AutoCloseable {

	private static final long serialVersionUID = 1L;

	private final SerializableObject lock;

	@GuardedBy("lock")
	private volatile boolean isReady;

	public InitializationGuard() {
		this.lock = new SerializableObject();
		this.isReady = false;
	}

	@Override
	public void close() {
		synchronized (lock) {
			isReady = true;
			lock.notifyAll();
		}
	}

	/**
	 * Blocks until the resource is initialized or initialization has failed.
	 */
	public void awaitInitialization() {
		synchronized (lock) {
			if (isReady) {
				return;
			}
			try {
				lock.wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}

