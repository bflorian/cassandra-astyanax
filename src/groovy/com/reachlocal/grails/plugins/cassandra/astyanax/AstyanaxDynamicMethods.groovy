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

import com.netflix.astyanax.thrift.model.ThriftColumnOrSuperColumnListImpl
import com.netflix.astyanax.thrift.ThriftColumnFamilyMutationImpl
import com.netflix.astyanax.thrift.model.ThriftRowsListImpl
import com.netflix.astyanax.thrift.ThriftKeyspaceImpl
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.thrift.AbstractThriftMutationBatchImpl
import com.netflix.astyanax.thrift.AbstractThriftColumnMutationImpl

/**
 * @author: Bob Florian
 */
class AstyanaxDynamicMethods
{
	static void addAll()
	{

		ThriftKeyspaceImpl.metaClass.prepareColumnMutation = {String columnFamily, rowKey, column ->
			delegate.prepareColumnMutation(new ColumnFamily(columnFamily, StringSerializer.get(), StringSerializer.get()), rowKey, column)
		}

		ThriftKeyspaceImpl.metaClass.prepareQuery = {String columnFamily ->
			delegate.prepareQuery(new ColumnFamily(columnFamily, StringSerializer.get(), StringSerializer.get()))
		}

		AbstractThriftMutationBatchImpl.metaClass.withRow = {String columnFamily, rowKey ->
			delegate.withRow(new ColumnFamily(columnFamily, StringSerializer.get(), StringSerializer.get()), rowKey)
		}

		AbstractThriftColumnMutationImpl.metaClass.putValue = {value ->
			delegate.putValue(value, null)
		}

		ThriftColumnOrSuperColumnListImpl.metaClass.toMap = {
			def result = [:]
			delegate.iterator().each {
				result[it.name] = it
			}
			result
		}

		ThriftColumnOrSuperColumnListImpl.metaClass.toStringMap = {
			def result = [:]
			delegate.iterator().each {
				result[it.name] = it.stringValue
			}
			result
		}

		ThriftColumnFamilyMutationImpl.metaClass.putColumn = {name, value ->
			delegate.putColumn(name, value, null)
		}

		ThriftColumnFamilyMutationImpl.metaClass.putColumns = {Map columns ->
			columns.each {key, value ->
				delegate.putColumn(key, value, null)
			}
		}

		ThriftColumnFamilyMutationImpl.metaClass.incrementCounterColumn = {name, value=1 ->
			delegate.incrementCounterColumn(name, value)
		}

		ThriftColumnFamilyMutationImpl.metaClass.incrementCounterColumns = {Map columns ->
			columns.each {key, value ->
				delegate.incrementCounterColumn(key, value ?: 1)
			}
		}

		ThriftColumnOrSuperColumnListImpl.metaClass.get = {String name ->
			delegate.getColumnByName(name)
		}
	}
}
