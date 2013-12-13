/*
 * Copyright 2012 ReachLocal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reachlocal.grails.plugins.cassandra.test

import org.junit.Test
import static org.junit.Assert.*
import com.reachlocal.grails.plugins.cassandra.test.util.TestSchema
import com.netflix.astyanax.model.ConsistencyLevel

/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethodConsistencyLevelTests 
{

	def astyanaxService
	def prefix = UUID.randomUUID()

	public AstyanaxPersistenceMethodConsistencyLevelTests()
	{
		TestSchema.initialize(astyanaxService)
	}

	@Test
	void testPrepareMutationBatchString()
	{
		def key = rowKey("testPrepareMutationBatchString")
		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, "CL_ALL")
			mapping.putColumn(m, columnFamily(keyspace), key, "name", "Test User 1")
			mapping.execute(m)
		}
	}

	@Test
	void testPrepareMutationBatchObject()
	{
		def key = rowKey("testPrepareMutationBatchString")
		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, ConsistencyLevel.CL_ALL)
			mapping.putColumn(m, columnFamily(keyspace), key, "name", "Test User 1")
			mapping.execute(m)
		}
	}

	@Test
	void testPrepareMutationBatchInvalidString()
	{
		def exceptionOccured = false
		def key = rowKey("testPrepareMutationBatchString")
		try {
			astyanaxService.withKeyspace() {keyspace ->
				def m = mapping.prepareMutationBatch(keyspace, "CL_EVERYTHING")
				mapping.putColumn(m, columnFamily(keyspace), key, "name", "Test User 1")
				mapping.execute(m)
			}
		}
		catch (IllegalArgumentException e) {
			exceptionOccured = true
		}
		assertTrue exceptionOccured
	}

	@Test
	void testGetColumn()
	{
		def key = rowKey("testGetColumn")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumn(m, columnFamily(keyspace), key, "name", "Test User 1")
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def col = mapping.getColumn(keyspace, columnFamily(keyspace), key, "name", "CL_ONE")
			assertEquals "Test User 1", mapping.stringValue(col)

			col = mapping.getColumn(keyspace, columnFamily(keyspace), key, "name", ConsistencyLevel.CL_ONE)
			assertEquals "Test User 1", mapping.stringValue(col)
		}
	}

	private rowKey(name)
	{
		"${prefix}-${name}".toString()
	}

	private getMapping()
	{
		astyanaxService.orm
	}

	private columnFamily(keyspace)
	{
		mapping.columnFamily(keyspace, "User")
	}

	private counterColumnFamily(keyspace)
	{
		mapping.columnFamily(keyspace, "User_CTR")
	}
}
