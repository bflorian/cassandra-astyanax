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

import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer

/**
 * @author: Bob Florian
 */
class AstyanaxDynamicMethodsTests extends GroovyTestCase
{
	def astyanaxService

	void testThriftKeyspaceImpl_prepareColumnMutation()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def m = keyspace.prepareColumnMutation("User", key, "name")
			m.putValue("Joe Smith", null).execute()

			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			assertEquals("Joe Smith", u.getColumnByName("name").stringValue)
		}
	}

	void testThriftKeyspaceImpl_prepareQuery()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			def cols = [name: "Jane Doe", city: "Olney", state: "MD"]
			m.withRow(cf, key).putColumns(cols)
			m.execute()

			def u = keyspace.prepareQuery("User").getKey(key).execute().result
			println u.getColumnByName("name").stringValue
			assertEquals(cols.name, u.getColumnByName("name").stringValue)
		}
	}

	void testAbstractThriftMutationBatchImpl_withRow()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def m = keyspace.prepareMutationBatch()
			def cols = [name: "John Doe", city: "Olney", state: "MD"]
			m.withRow("User", key).putColumns(cols)
			m.execute()

			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			println u.getColumnByName("name").stringValue
			assertEquals(cols.name, u.getColumnByName("name").stringValue)
		}
	}

	void testAbstractThriftColumnMutationImpl_putValue()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def m = keyspace.prepareColumnMutation(new ColumnFamily("User", StringSerializer.get(), StringSerializer.get()), key, "name")
			m.putValue("Joe Smith").execute()

			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			assertEquals("Joe Smith", u.getColumnByName("name").stringValue)
		}
	}

	void testThriftColumnOrSuperColumnListImpl_toMap()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			m.withRow(cf, key).putColumn("name","Jane Doe",null).putColumn("city","Olney",null).putColumn("state","MD",null)
			m.execute()

			def u = keyspace.prepareQuery("User").getKey(key).execute().result
			def map = u.toMap()
			assertEquals("Jane Doe", map.name.stringValue)
		}
	}

	void testThriftColumnFamilyMutationImpl_putColumn()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			m.withRow(cf, key).putColumn("name","Jane Doe")
			m.execute()

			def u = keyspace.prepareQuery("User").getKey(key).execute().result
			assertEquals("Jane Doe", u.getColumnByName("name").stringValue)
		}
	}

	void testThriftColumnFamilyMutationImpl_putColumns()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			def cols = [name: "Jane Doe", city: "Olney", state: "MD"]
			m.withRow(cf, key).putColumns(cols)
			m.execute()

			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			println u.getColumnByName("name").stringValue
			assertEquals(cols.name, u.getColumnByName("name").stringValue)
			assertEquals(cols.city, u.getColumnByName("city").stringValue)
			assertEquals(cols.state, u.getColumnByName("state").stringValue)
		}
	}

	void testThriftColumnFamilyMutationImpl_incrementCounterColumn()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User_CTR", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			m.withRow(cf, key).incrementCounterColumn("colors")
			m.withRow(cf, key).incrementCounterColumn("flavors", 3)
			m.execute()

			def u = keyspace.prepareQuery("User_CTR").getKey(key).execute().result
			assertEquals(1, u.getColumnByName("colors").longValue)
			assertEquals(3, u.getColumnByName("flavors").longValue)

			m = keyspace.prepareMutationBatch()
			m.withRow(cf, key).incrementCounterColumn("colors", 2)
			m.withRow(cf, key).incrementCounterColumn("flavors")
			m.execute()

			u = keyspace.prepareQuery("User_CTR").getKey(key).execute().result
			assertEquals(3, u.getColumnByName("colors").longValue)
			assertEquals(4, u.getColumnByName("flavors").longValue)
		}
	}

	void testThriftColumnFamilyMutationImpl_incrementCounterColumns()
	{
		def key =  UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def cf = new ColumnFamily("User_CTR", StringSerializer.get(), StringSerializer.get())
			def m = keyspace.prepareMutationBatch()
			def cols = [colors: 1, flavors: 5]
			m.withRow(cf, key).incrementCounterColumns(cols)
			m.execute()

			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			assertEquals(1, u.getColumnByName("colors").longValue)
			assertEquals(5, u.getColumnByName("flavors").longValue)

			cols = [colors: 2, flavors: 6]
			m.withRow(cf, key).incrementCounterColumns(cols)
			m.execute()

			u = keyspace.prepareQuery(cf).getKey(key).execute().result
			assertEquals(3, u.getColumnByName("colors").longValue)
			assertEquals(11, u.getColumnByName("flavors").longValue)
		}
	}

	void testThriftColumnOrSuperColumnListImpl_get()
	{
		def key = UUID.randomUUID().toString()

		astyanaxService.withKeyspace() {keyspace ->
			def m = keyspace.prepareColumnMutation(new ColumnFamily("User", StringSerializer.get(), StringSerializer.get()), key, "name")
			m.putValue("Sally Smith", null).execute()

			def cf = new ColumnFamily("User", StringSerializer.get(), StringSerializer.get())
			def u = keyspace.prepareQuery(cf).getKey(key).execute().result
			assertEquals("Sally Smith", u.get("name").stringValue)
			assertEquals("Sally Smith", u["name"].stringValue)
			assertEquals("Sally Smith", u.name.stringValue)
		}
	}
}
