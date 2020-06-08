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

package org.apache.flink.formats.hadoop.bulk.committer;

import org.apache.flink.formats.hadoop.bulk.HadoopFileCommitter;

import org.apache.flink.util.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The Hadoop file committer that directly rename the in-progress file
 * to the target file. For FileSystem like S3, renaming may lead to
 * additional copies.
 */
public class HadoopRenameFileCommitter implements HadoopFileCommitter {

	private final Configuration configuration;

	private final Path targetFilePath;

	private final Path inProgressFilePath;

	private final Path pendingFilePath;

	public HadoopRenameFileCommitter(Configuration configuration, Path targetFilePath) {
		this.configuration = configuration;
		this.targetFilePath = targetFilePath;

		checkArgument(targetFilePath.isAbsolute(), "Target file must be absolute");
		this.inProgressFilePath = generateInProgressFilePath();
		this.pendingFilePath = generatePendingFilePath();

		deleteIfExists(inProgressFilePath);
	}

	@Override
	public Path getTargetFilePath() {
		return targetFilePath;
	}

	@Override
	public Path getInProgressFilePath() {
		return inProgressFilePath;
	}

	@Override
	public void preCommit() throws IOException {
		rename(inProgressFilePath, pendingFilePath, true);
	}

	@Override
	public void commit() throws IOException {
		rename(pendingFilePath, targetFilePath, true);
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		rename(pendingFilePath, targetFilePath, false);
	}

	private void rename(Path from, Path to, boolean assertFileExists) throws IOException {
		FileSystem fileSystem = FileSystem.get(from.toUri(), configuration);

		if (!fileSystem.exists(from)) {
			if (assertFileExists) {
				throw new IOException(String.format("In progress file(%s) not exists.", from));
			} else {
				// By pass the re-commit if source file not exists.
				// TODO: in the future we may also need to check if the target file exists.
				return;
			}
		}

		try {
			// If file exists, it will be overwritten.
			fileSystem.rename(from, to);
		} catch (IOException e) {
			throw new IOException(
				String.format("Could not commit file from %s to %s", from, to),
				e);
		}
	}

	private Path generateInProgressFilePath() {
		Path parent = targetFilePath.getParent();
		String name = targetFilePath.getName();

		return new Path(parent, "." + name + ".inprogress");
	}

	private Path generatePendingFilePath() {
		Path parent = targetFilePath.getParent();
		String name = targetFilePath.getName();

		return new Path(parent, "." + name + ".pending");
	}

	private void deleteIfExists(Path path) {
		try {
			FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);

			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
			}
		} catch (IOException e) {
			ExceptionUtils.rethrow(
				e,
				String.format("Failed to delete the existing in-progress file: %s", path));
		}
	}
}
