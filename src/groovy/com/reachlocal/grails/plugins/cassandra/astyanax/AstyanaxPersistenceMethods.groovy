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

package com.reachlocal.grails.plugins.cassandra.astyanax

import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.reachlocal.grails.plugins.cassandra.OrmPersistenceMethods

/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethods implements OrmPersistenceMethods
{
	// Read operations
	def columnFamily(String name)
	{
		new ColumnFamily(name.toString(), StringSerializer.get(), StringSerializer.get())
	}

	Iterable getRow(Object client, Object columnFamily, Object rowKey)
	{
		def cols = client.prepareQuery(columnFamily).getKey(rowKey).execute().result
		cols.isEmpty() ? null : cols
	}

	Iterable getRows(Object client, Object columnFamily, Collection rowKeys)
	{
		client.prepareQuery(columnFamily).getKeySlice(rowKeys).execute().result*.columns
	}

	Iterable getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames)
	{
		client.prepareQuery(columnFamily).getKeySlice(rowKeys).withColumnSlice(columnNames).execute().result*.columns
	}

	Iterable getRowsWithEqualityIndex(client, columnFamily, properties, max)
	{
		def exp = properties.collect {name, value ->
			columnFamily.newIndexClause().whereColumn(name).equals().value(value)
		}

		client.prepareQuery(columnFamily)
				.searchWithIndex()
				.setRowLimit(max)
				.addPreparedExpressions(exp)
				.execute()
				.result*.column
	}

	Iterable getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max)
	{
		client.prepareQuery(columnFamily).getKey(rowKey)
				.withColumnRange(start, finish, reversed, max)
				.execute()
				.result
	}

	Iterable getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames)
	{
		client.prepareQuery(columnFamily).getKey(rowKey)
				.withColumnSlice(columnNames)
				.execute()
				.result
	}


	// Write operations
	def prepareMutationBatch(client)
	{
		client.prepareMutationBatch()
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		mutationBatch.withRow(columnFamily, rowKey).deleteColumn(columnName)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		mutationBatch.withRow(columnFamily, rowKey).putColumns(columnMap)
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		mutationBatch.deleteRow([columnFamily], rowKey)
	}

	def execute(mutationBatch)
	{
		mutationBatch.execute()
	}

	// Column data values
	def getColumn(row, name)
	{
		row.getColumnByName(name)
	}

	def name(column)
	{
		column.name
	}
	
	String stringValue(column)
	{
		column.stringValue
	}

	byte[] byteArrayValue(column)
	{
		column.byteArrayValue
	}
}
