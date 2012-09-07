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
import com.netflix.astyanax.model.ConsistencyLevel

/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethods
{
	// Read operations
	def columnFamily(String name)
	{
		new ColumnFamily(name.toString(), StringSerializer.get(), StringSerializer.get())
	}

	def columnFamilyName(columnFamily)
	{
		columnFamily.name
	}

	def getRow(Object client, Object columnFamily, Object rowKey, consistencyLevel)
	{
		def cols = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey).execute().result
		cols.isEmpty() ? null : cols
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys).execute().result
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys).withColumnSlice(columnNames).execute().result
	}

	def getRowsColumnRange(Object client, Object columnFamily, Collection rowKeys, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys)
				.withColumnRange(start, finish, reversed, max)
				.execute()
				.result
	}

	def getRowsWithEqualityIndex(client, columnFamily, properties, max, consistencyLevel)
	{
		def exp = properties.collect {name, value ->
			columnFamily.newIndexClause().whereColumn(name).equals().value(value)
		}

		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.searchWithIndex()
				.setRowLimit(max)
				.addPreparedExpressions(exp)
				.execute()
				.result
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties, consistencyLevel)
	{
		def clause = properties.collect {name, value -> "${name} = '${value}'"}.join(" AND ")
		def query = "SELECT COUNT(*) FROM ${columnFamily.name} WHERE ${clause}"

		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.withCql(query)
				.execute()
				.result
				.rows
				.getRowByIndex(0)
				.columns
				.getColumnByIndex(0)
				.longValue
	}
	
	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily).getKey(rowKey), consistencyLevel)
				.withColumnRange(start, finish, reversed, max)
				.execute()
				.result
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, consistencyLevel)
	{
		def row = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
		if (start || finish) {
			row = row.withColumnRange(start, finish, false, Integer.MAX_VALUE)
		}
		row.getCount()
				.execute()
				.result
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
				.withColumnSlice(columnNames)
				.execute()
				.result
	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName, consistencyLevel)
	{
		injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
				.getColumn(columnName)
				.execute()
				.result
	}

	def prepareMutationBatch(client, ConsistencyLevel consistencyLevel)
	{
		def m = client.prepareMutationBatch()
		if (consistencyLevel) {
			m.setConsistencyLevel(consistencyLevel)
		}
		return m
	}

	def prepareMutationBatch(client, consistencyLevel)
	{
		def m = client.prepareMutationBatch()
		if (consistencyLevel) {
			m.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel))
		}
		return m
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		mutationBatch.withRow(columnFamily, rowKey).deleteColumn(columnName)
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value)
	{
		mutationBatch.withRow(columnFamily, rowKey).putColumn(name, value)
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value, ttl)
	{
		mutationBatch.withRow(columnFamily, rowKey).putColumn(name, value, ttl)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		mutationBatch.withRow(columnFamily, rowKey).putColumns(columnMap)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap)
	{
		def r = mutationBatch.withRow(columnFamily, rowKey)
		if (ttlMap instanceof Number) {
			columnMap.each {k, v ->
				r.putColumn(k, v, ttlMap)
			}
		}
		else {
			columnMap.each {k, v ->
				r.putColumn(k, v, ttlMap[k])
			}
		}
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, columnName, value=1)
	{
		mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, value)
	}

	void incrementCounterColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumns(columnMap)
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		mutationBatch.deleteRow([columnFamily], rowKey)
	}

	def execute(mutationBatch)
	{
		mutationBatch.execute()
	}

	def getRow(rows, key)
	{
		rows.getRow(key).columns
	}

	def getRowKey(row)
	{
		row.key
	}

	def getColumns(row)
	{
		row.columns
	}

	def getColumn(row, name)
	{
		row.getColumnByName(name)
	}

	def getColumnByIndex(row, index)
	{
		row.getColumnByIndex(index)
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

	def longValue(column)
	{
		column.longValue
	}

	private injectConsistencyLevel(query, ConsistencyLevel consistencyLevel)
	{
		if (consistencyLevel) {
			query.setConsistencyLevel(consistencyLevel)
		}
		return query
	}

	private injectConsistencyLevel(query, String consistencyLevel)
	{
		if (consistencyLevel) {
			def cl = ConsistencyLevel.valueOf(consistencyLevel)
			if (cl) {
				query.setConsistencyLevel(cl)
			}
			else {
				throw new IllegalArgumentException("'${consistencyLevel}' is not a valid ConsistencyLevel, must be one of [CL_ONE, CL_QUORUM, CL_ALL, CL_ANY, CL_EACH_QUORUM, CL_LOCAL_QUORUM, CL_TWO, CL_THREE]")
			}
		}
		return query
	}
}
