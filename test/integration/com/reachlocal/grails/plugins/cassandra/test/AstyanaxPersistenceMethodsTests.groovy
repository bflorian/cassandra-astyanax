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

/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethodsTests extends GroovyTestCase
{
	def astyanaxService
	def prefix = UUID.randomUUID()

	void testPutAndGetColumn()
	{
		def key = rowKey("testPutAndGetColumn")
		
		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)			
			mapping.putColumn(m, columnFamily, key, "name", "Test User 1")
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily, key)
			assertEquals("Test User 1", mapping.stringValue(mapping.getColumn(row, "name")))
		}
	}

	void testPutAndGetRow()
	{
		def key = rowKey("testPutAndGetRow")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 1"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily, key)
			assertEquals("Test User 1", mapping.stringValue(mapping.getColumn(row, "name")))
		}
	}

	void testPutAndGetRows()
	{
		def key = rowKey("testPutAndGetRows")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			for (i in 1..5) {
				mapping.putColumns(m, columnFamily, "${key}-${i}".toString(), [name: "Test User ${i}".toString()])
			}
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def keys = ["$key-1".toString(),"$key-3".toString(),"$key-5".toString()]
			def rows = mapping.getRows(keyspace, columnFamily, keys)
			assertEquals 3, rows.size()

			def row = mapping.getRow(rows, keys[0])
			assertNotNull row
			assertEquals "Test User 1", row.getColumnByName("name").stringValue
		}
	}

	void testPutAndGetRowsColumnSlice()
	{
		def key = rowKey("testPutAndGetRowsColumnSlice")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			for (i in 1..5) {
				mapping.putColumns(m, columnFamily, "${key}-${i}".toString(), [
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

		astyanaxService.execute() {keyspace ->
			def keys = ["$key-1".toString(),"$key-3".toString()]
			def rows = mapping.getRowsColumnSlice(keyspace, columnFamily, keys, ["name","x5"])
			assertEquals 2, rows.size()
			assertNotNull rows.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 3"}
			assertNull rows.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 2"}
			
			def row = rows.find{mapping.stringValue(mapping.getColumn(it, "name")) == "Test User 1"}
			assertNotNull row
			assertEquals "five-1", row.x5.stringValue
			assertNull row.x1
		}
	}
	
	def testPutAndGetColumnRange()
	{
		def key = rowKey("testPutAndGetColumnRange")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 2", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def range = mapping.getColumnRange(keyspace, columnFamily, key, null, null, false, 10)
			assertEquals 6, range.size()

			range = mapping.getColumnRange(keyspace, columnFamily, key, null, null, false, 3)
			assertEquals 3, range.size()

			range = mapping.getColumnRange(keyspace, columnFamily, key, "x1", "x4", false, 10)
			assertEquals 4, range.size()
			assertEquals "one", mapping.stringValue(range.getColumnByIndex(0))

			range = mapping.getColumnRange(keyspace, columnFamily, key, "x4", "x3", true, 10)
			assertEquals 2, range.size()
			assertEquals "four", mapping.stringValue(range.getColumnByIndex(0))
		}
	}

	def testPutAndGetColumnSlice()
	{
		def key = rowKey("testPutAndGetColumnSlice")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 2", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def slice = mapping.getColumnSlice(keyspace, columnFamily, key, ["name","x5"])
			assertEquals 2, slice.size()
			assertEquals "five", mapping.stringValue(slice.getColumnByIndex(1))

			slice = mapping.getColumnSlice(keyspace, columnFamily, key, ["x2"])
			assertEquals 1, slice.size()
			assertEquals "two", mapping.stringValue(slice.getColumnByIndex(0))
		}
	}
	
	void testPutAndDeleteColumn()
	{
		def key = rowKey("testPutAndDeleteColumn")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 1", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.deleteColumn(m, columnFamily, key, "x2")
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily, key)
			assertEquals "Test User 1", mapping.stringValue(mapping.getColumn(row, "name"))
			assertEquals "four", mapping.stringValue(mapping.getColumn(row, "x4"))
			assertNull mapping.getColumn(row, "x2")
		}		
	}

	void testPutAndDeleteRow()
	{
		def key = rowKey("testPutAndDeleteRow")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 1", x1:"one", x2:"two", x3:"three", x4:"four", x5:"five"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.deleteRow(m, columnFamily, key)
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily, key)
			assertNull row
		}
	}

	void testByteArrayValue()
	{
		def key = rowKey("testByteArrayValue")

		astyanaxService.execute() {keyspace ->
			def m = mapping.prepareMutationBatch(keyspace)
			mapping.putColumns(m, columnFamily, key, [name: "Test User 1"])
			mapping.execute(m)
		}

		astyanaxService.execute() {keyspace ->
			def row = mapping.getRow(keyspace, columnFamily, key)
			assertEquals("Test User 1".bytes, mapping.byteArrayValue(mapping.getColumn(row, "name")))
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

	private getColumnFamily()
	{
		mapping.columnFamily("User")
	}
}
