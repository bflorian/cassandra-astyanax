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
import com.netflix.astyanax.serializers.TimeUUIDSerializer
import com.netflix.astyanax.serializers.UUIDSerializer
import com.reachlocal.grails.plugins.cassandra.mapping.PersistenceProvider;
/**
 * @author: Bob Florian
 */
class AstyanaxPersistenceMethods implements PersistenceProvider
{
	def log

	private logTime(long t0, name) {
		log.trace "Astyanax.$name ${System.currentTimeMillis() - t0} msec"
	}

	// Read operations
	def columnTypes(Object client, String name)
	{
		long t0 = System.currentTimeMillis()
		def result = [:]
		def cf = client.describeKeyspace().getColumnFamily(name)
		cf.columnDefinitionList.each {
			result[it.name] = dataType(it.validationClass)
		}
		logTime(t0, "columnTypes")
		result
	}

	def columnFamily(Object client, String name)
	{
		long t0 = System.currentTimeMillis()
		def cf = client.describeKeyspace().getColumnFamily(name)
		def rowSerializer = dataSerializer(cf?.keyValidationClass)
		def columnNameSerializer = dataSerializer(cf?.comparatorType)
		def result = new ColumnFamily(name.toString(), rowSerializer, columnNameSerializer)
		logTime(t0, "columnFamily")
		result
	}

	private static dataSerializer(String clazz) {
		clazz ? SERIALIZERS[dataType(clazz)] ?: StringSerializer.get() : StringSerializer.get()
	}

	private static SERIALIZERS = [
	    UUID : UUIDSerializer.get(),
		TimeUUID : TimeUUIDSerializer.get()
	]

	private static dataType(String s) {
		final pat = ~/.*\.([a-z,A-Z,0-9]+)Type\)?$/
		pat.matcher(s).replaceAll('$1')
	}

	def indexIsTimeUuid(indexColumnFamily) {
		indexColumnFamily.columnSerializer instanceof TimeUUIDSerializer
	}

	def keyIsTimeUuid(columnFamily) {
		columnFamily.keySerializer instanceof TimeUUIDSerializer
	}

	def indexIsReversed(Object client, String indexColumnFamilyName) {
		long t0 = System.currentTimeMillis()
		def cf = client.describeKeyspace().getColumnFamily(indexColumnFamilyName)
		logTime(t0, "indexIsReversed")
		cf?.comparatorType?.startsWith("org.apache.cassandra.db.marshal.ReversedType")
	}

	def columnFamilyName(columnFamily)
	{
		columnFamily.name
	}

	def getRow(Object client, Object columnFamily, Object rowKey, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def cols = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey).execute().result
		def result = cols.isEmpty() ? null : cols
		logTime(t0, "getRow")
		result
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys).execute().result
		logTime(t0, "getRows")
		result
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys).withColumnSlice(columnNames).execute().result
		logTime(t0, "getRowsColumnSlice")
		result
	}

	def getRowsColumnRange(Object client, Object columnFamily, Collection rowKeys, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKeySlice(rowKeys)
				.withColumnRange(start, finish, reversed, max)
				.execute()
				.result
		logTime(t0, "getRowsColumnRange")
		result
	}

	def getRowsWithEqualityIndex(client, columnFamily, properties, max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def exp = properties.collect {name, value ->
			columnFamily.newIndexClause().whereColumn(name).equals().value(value)
		}

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.searchWithIndex()
				.setRowLimit(max)
				.addPreparedExpressions(exp)
				.execute()
				.result
		logTime(t0, "getRowsWithEqualityIndex")
		result
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def clause = properties.collect {name, value -> "${name} = '${value}'"}.join(" AND ")
		def query = "SELECT COUNT(*) FROM ${columnFamily.name} WHERE ${clause}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.withCql(query)
				.execute()
				.result
				.rows
				.getRowByIndex(0)
				.columns
				.getColumnByIndex(0)
				.longValue
		logTime(t0, "countRowsWithEqualityIndex")
		result
	}

	def getRowsWithCqlWhereClause(client, columnFamily, clause, max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT * FROM ${columnFamily.name} WHERE ${clause} LIMIT ${max}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.withCql(query)
				.execute()
				.result
		logTime(t0, "getRowsWithCqlWhereClause")
		result
	}

	def getRowsColumnSliceWithCqlWhereClause(client, columnFamily, clause, max, columns, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT ${columns.join(', ')} FROM ${columnFamily.name} WHERE ${clause} LIMIT ${max}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.withCql(query)
				.execute()
				.result
		logTime(t0, "getRowsColumnSliceWithCqlWhereClause")
		result
	}

	def countRowsWithCqlWhereClause(client, columnFamily, clause, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def query = "SELECT COUNT(*) FROM ${columnFamily.name} WHERE ${clause}"

		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel)
				.withCql(query)
				.execute()
				.result
				.rows
				.getRowByIndex(0)
				.columns
				.getColumnByIndex(0)
				.longValue
		logTime(t0, "countRowsWithCqlWhereClause")
		result
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
				.withColumnRange(start, finish, reversed, max)
				.execute()
				.result
		logTime(t0, "getColumnRange")
		result
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def row = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
		if (start || finish) {
			row = row.withColumnRange(start, finish, false, Integer.MAX_VALUE)
		}
		def result = row.getCount()
				.execute()
				.result
		logTime(t0, "countColumnRange")
		result
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
				.withColumnSlice(columnNames)
				.execute()
				.result
		logTime(t0, "getColumnSlice")
		result
	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName, consistencyLevel)
	{
		long t0 = System.currentTimeMillis()
		def result = injectConsistencyLevel(client.prepareQuery(columnFamily), consistencyLevel).getKey(rowKey)
				.getColumn(columnName)
				.execute()
				.result
		logTime(t0, "getColumn")
		result
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
		def r = mutationBatch.withRow(columnFamily, rowKey)
		columnMap.each {k, v ->
			if (v != null) {
				r.putColumn(k, v, null)
			}
			else {
				r.deleteColumn(k)
			}
		}
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap)
	{
		def r = mutationBatch.withRow(columnFamily, rowKey)
		if (ttlMap instanceof Number) {
			columnMap.each {k, v ->
				if (v != null) {
					r.putColumn(k, v, ttlMap)
				}
				else {
					r.deleteColumn(k)
				}
			}
		}
		else {
			columnMap.each {k, v ->
				if (v != null) {
					r.putColumn(k, v, ttlMap[k])
				}
				else {
					r.deleteColumn(k)
				}
			}
		}
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, 1)
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, columnName, Long value)
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
		long t0 = System.currentTimeMillis()
		def result = mutationBatch.execute()
		logTime(t0, "execute")
		result
	}

	def getRow(rows, key)
	{
		rows.getRow(key).columns
	}

	def getRowKey(row)
	{
		row.key
	}

	Iterable getColumns(row)
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

	UUID uuidValue(column)
	{
		column.UUIDValue
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
