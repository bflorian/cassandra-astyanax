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

import com.reachlocal.grails.plugins.cassandra.astyanax.AstyanaxPersistenceMethods
import com.reachlocal.grails.plugins.cassandra.test.util.TestSchema
import com.netflix.astyanax.model.ConsistencyLevel
import org.junit.Test
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethodsTests
{
	def astyanaxService
	def prefix = UUID.randomUUID()

	public AstyanaxPersistenceMethodsTests()
	{
		TestSchema.initialize(astyanaxService)
	}

	@Test
	void testPutAndGetColumn()
	{
		def key = rowKey("testPutAndGetColumn")
		
		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumn(m, columnFamily(keyspace), key, "name", "Test User 1")
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def col = mapping.getColumn(keyspace, columnFamily(keyspace), key, "name", null)
			assertEquals "Test User 1", mapping.stringValue(col)
		}
	}

	@Test
	void testPutAndGetRow()
	{
		def key = rowKey("testPutAndGetRow")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, "CL_ALL")
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 1"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily(keyspace), key, "CL_ONE")
			assertEquals("Test User 1", mapping.stringValue(mapping.getColumn(row, "name")))
		}
	}

	@Test
	void testPutAndGetRows()
	{
		def key = rowKey("testPutAndGetRows")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, ConsistencyLevel.CL_ALL)
			for (i in 1..5) {
				mapping.putColumns(m, columnFamily(keyspace), "${key}-${i}".toString(), [name: "Test User ${i}".toString()])
			}
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def keys = ["$key-1".toString(),"$key-3".toString(),"$key-5".toString()]
			def rows = mapping.getRows(keyspace, columnFamily(keyspace), keys, "CL_ONE")
			assertEquals 3, rows.size()

			def row = mapping.getRow(rows, keys[0])
			assertNotNull row
			assertEquals "Test User 1", row.getColumnByName("name").stringValue
		}
	}

	@Test
	void testPutAndGetRowsWithEqualityIndex()
	{
		def key = rowKey("testPutAndGetRowsWithEqualityIndex")
		def state = UUID.randomUUID().toString()
		
		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			for (j in 1..3) {
				for (i in 1..(2*j)) {
					mapping.putColumns(m, columnFamily(keyspace), "${key}-${i}-${j}".toString(), [
							name: "Test User ${i}".toString(), 
							city: "City ${j}".toString(),
							state: state
					])
				}
			}
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def rows = mapping.getRowsWithEqualityIndex(keyspace, columnFamily(keyspace), [city: "City 1", state:  state], 100, null)
			assertEquals 2, rows.size()

			rows = mapping.getRowsWithEqualityIndex(keyspace, columnFamily(keyspace), [city: "City 3", state:  state], 100, null)
			assertEquals 6, rows.size()
		}
	}

	@Test
	void testPutAndCountRowsWithEqualityIndex()
	{
		def key = rowKey("testPutAndCountRowsWithEqualityIndex")
		def state = UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			for (j in 1..3) {
				for (i in 1..(2*j)) {
					mapping.putColumns(m, columnFamily(keyspace), "${key}-${i}-${j}".toString(), [
							name: "Test User ${i}".toString(),
							city: "City ${j}".toString(),
							state: state
					])
				}
			}
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def count = mapping.countRowsWithEqualityIndex(keyspace, columnFamily(keyspace), [city: "City 1", state:  state], null)
			assertEquals 2, count

			count = mapping.countRowsWithEqualityIndex(keyspace, columnFamily(keyspace), [city: "City 2", state:  state], null)
			assertEquals 4, count
		}
	}

	@Test
	void testPutAndGetRowsColumnSlice()
	{
		def key = rowKey("testPutAndGetRowsColumnSlice")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			for (i in 1..5) {
				mapping.putColumns(m, columnFamily(keyspace), "${key}-${i}".toString(), [
						name: "Test User ${i}".toString(),
						x1:"one-$i".toString(),
						x2:"two-$i".toString(),
						x3:"three-$i".toString(),
						x4:"four-$i".toString(),
						x5:"five-$i".toString()
				])
			}
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def keys = ["$key-1".toString(),"$key-3".toString()]
			def rows = mapping.getRowsColumnSlice(keyspace, columnFamily(keyspace), keys, ["name","x5"], null)
			assertEquals 2, rows.size()
			assertNotNull rows*.columns.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 3"}
			assertNull rows*.columns.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 2"}
			
			def row = rows*.columns.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 1"}
			assertNotNull row
			assertEquals "five-1", row.x5.stringValue
			assertNull row.x1
		}
	}

	@Test
	void testPutAndGetColumnRange()
	{
		def key = rowKey("testPutAndGetColumnRange")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 2", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def range = mapping.getColumnRange(keyspace, columnFamily(keyspace), key, null, null, false, 10, null)
			assertEquals 6, range.size()

			range = mapping.getColumnRange(keyspace, columnFamily(keyspace), key, null, null, false, 3, null)
			assertEquals 3, range.size()

			range = mapping.getColumnRange(keyspace, columnFamily(keyspace), key, "x1", "x4", false, 10, null)
			assertEquals 4, range.size()
			assertEquals "one", mapping.stringValue(range.getColumnByIndex(0))

			range = mapping.getColumnRange(keyspace, columnFamily(keyspace), key, "x4", "x3", true, 10, null)
			assertEquals 2, range.size()
			assertEquals "four", mapping.stringValue(range.getColumnByIndex(0))
		}
	}

	@Test
	void testPutAndCountColumnRange()
	{
		def key = rowKey("testPutAndCountColumnRange")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 2", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def count = mapping.countColumnRange(keyspace, columnFamily(keyspace), key, null, null, null)
			assertEquals 6, count

			count = mapping.countColumnRange(keyspace, columnFamily(keyspace), key, "x1", "x4", null)
			assertEquals 4, count

			count = mapping.countColumnRange(keyspace, columnFamily(keyspace), key, "x3", "x4", null)
			assertEquals 2, count
		}
	}

	@Test
	void testPutAndGetColumnSlice()
	{
		def key = rowKey("testPutAndGetColumnSlice")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 2", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def slice = mapping.getColumnSlice(keyspace, columnFamily(keyspace), key, ["name","x5"], null)
			assertEquals 2, slice.size()
			assertEquals "five", mapping.stringValue(slice.getColumnByIndex(1))

			slice = mapping.getColumnSlice(keyspace, columnFamily(keyspace), key, ["x2"], null)
			assertEquals 1, slice.size()
			assertEquals "two", mapping.stringValue(slice.getColumnByIndex(0))
		}
	}

	@Test
	void testPutAndDeleteColumn()
	{
		def key = rowKey("testPutAndDeleteColumn")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 1", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.deleteColumn(m, columnFamily(keyspace), key, "x2")
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily(keyspace), key, null)
			assertEquals "Test User 1", mapping.stringValue(mapping.getColumn(row, "name"))
			assertEquals "four", mapping.stringValue(mapping.getColumn(row, "x4"))
			assertNull mapping.getColumn(row, "x2")
		}		
	}

	@Test
	void testPutAndDeleteRow()
	{
		def key = rowKey("testPutAndDeleteRow")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 1", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.deleteRow(m, columnFamily(keyspace), key)
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily(keyspace), key, null)
			assertNull row
		}
	}

	@Test
	void testByteArrayValue()
	{
		def key = rowKey("testByteArrayValue")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.putColumns(m, columnFamily(keyspace), key, [name: "Test User 1"])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily(keyspace), key, null)
			def array1 = "Test User 1".bytes
			def array2 = mapping.byteArrayValue(mapping.getColumn(row, "name"))
			assertEquals(array1.size(), array2.size())
			array1.eachWithIndex {it, index ->
				assertEquals it, array2[index]
			}
		}
	}

	@Test
	void testIncrementAndGetColumn()
	{
		def key = rowKey("testIncrementAndGetColumn")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.incrementCounterColumn(m, counterColumnFamily(keyspace), key, "places")
			mapping.incrementCounterColumn(m, counterColumnFamily(keyspace), key, "books", 3)
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def col1 = mapping.getColumn(keyspace, counterColumnFamily(keyspace), key, "places", null)
			assertEquals 1, mapping.longValue(col1)
			def col2 = mapping.getColumn(keyspace, counterColumnFamily(keyspace), key, "books", null)
			assertEquals 3, mapping.longValue(col2)
		}
	}

	@Test
	void testIncrementAndGetColumns()
	{
		def key = rowKey("testIncrementAndGetColumns")

		astyanaxService.withKeyspace() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace, null)
			mapping.incrementCounterColumns(m, counterColumnFamily(keyspace), key, [places:2, books:5])
			mapping.execute(m)
		}

		astyanaxService.withKeyspace() {keyspace ->
			def col1 = mapping.getColumn(keyspace, counterColumnFamily(keyspace), key, "places", null)
			assertEquals 2, mapping.longValue(col1)
			def col2 = mapping.getColumn(keyspace, counterColumnFamily(keyspace), key, "books", null)
			assertEquals 5, mapping.longValue(col2)
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
