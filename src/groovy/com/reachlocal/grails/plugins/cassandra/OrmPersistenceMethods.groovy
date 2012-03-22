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

package com.reachlocal.grails.plugins.cassandra

import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer

/**
 * @author: Bob Florian
 */
interface OrmPersistenceMethods
{
	def columnFamily(String name);

	Iterable getRow(Object client, Object columnFamily, Object rowKey);

	Iterable getRows(Object client, Object columnFamily, Collection rowKeys);

	Iterable getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames);

	Iterable getRowsWithEqualityIndex(client, columnFamily, properties, max);

	Iterable getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max);

	Iterable getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames);

	def prepareMutationBatch(client);

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName);

	void putColumn(mutationBatch, columnFamily, rowKey, name, value);

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap);

	void deleteRow(mutationBatch, columnFamily, rowKey);

	def execute(mutationBatch);

	def getColumn(row, name);

	def name(column);

	String stringValue(column);

	byte[] byteArrayValue(column);
}
