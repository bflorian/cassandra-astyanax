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

import org.codehaus.groovy.grails.commons.ConfigurationHolder
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.util.RangeBuilder

import groovy.sql.Sql
import com.reachlocal.grails.plugins.cassandra.OrmPersistenceMethods

/**
 * @author Bob Florian
 *
 */
class AstyanaxService
{
	boolean transactional = false

	def port = ConfigurationHolder.config?.cassandra?.port ?: 9160
	def host = ConfigurationHolder.config?.cassandra?.host ?: "127.0.0.1"
	def seeds = ConfigurationHolder.config?.cassandra?.seeds ?: "${host}:${port}"
	def maxConsPerHost = ConfigurationHolder.config?.cassandra?.maxConsPerHost ?: 10
	def cluster = ConfigurationHolder.config?.cassandra?.cluster ?: "Test Cluster"
	def connectionPoolName = ConfigurationHolder.config?.cassandra?.connectionPoolName ?: "MyConnectionPool"
	def discoveryType = ConfigurationHolder.config?.cassandra?.discoveryType ?: com.netflix.astyanax.connectionpool.NodeDiscoveryType.NONE
	def defaultKeyspace = ConfigurationHolder.config?.cassandra?.keySpace ?: "AstyanaxTest"

	def cqlDriver = "org.apache.cassandra.cql.jdbc.CassandraDriver"
	/**
	 * Constructs an Astyanax context and passed execution to a closure
	 *
	 * @param keyspace name of the keyspace
	 * @param block closure to be executed
	 * @throws Exception
	 */
	void execute(keyspace=defaultKeyspace, block) throws Exception
	{
		def context = new AstyanaxContext.Builder()
				.forCluster(cluster)
				.forKeyspace(keyspace)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
				.setDiscoveryType(discoveryType)
		)
				.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(connectionPoolName)
				.setPort(port)
				.setMaxConnsPerHost(maxConsPerHost)
				.setSeeds(seeds)
		)
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start()

		try {
			block(context.entity)
		}
		finally {
			context.shutdown()
		}
	}

	/**
	 * Initialized a CQL JDBC connection
	 * 
	 * @param keyspace name of the keyspace
	 * @return initialized JDBC/CQL connection object
	 * @throws Exception
	 */
	Sql cql(keyspace=defaultKeyspace) throws Exception
	{
		Sql.newInstance("jdbc:cassandra://localhost:${port}/${keyspace}", cqlDriver)
	}

	/**
	 * Utility method to print out readable version of column family for debugging purposes
	 *
	 * @param names list of column family names to display
	 * @param keyspace name of the keyspace
	 * @param maxRows the maximum number of rows to print
	 * @param maxColumns the maximum number of columns to print for each row
	 * @param out the print writer to use, defaults to System.out
	 */
	void showColumnFamilies (Collection names, String keyspace=defaultKeyspace, Integer maxRows=50, Integer maxColumns=10, out=System.out) {
		names.each {String cf ->
			execute(keyspace) {ks ->
				out.println "${cf}:"
				ks.prepareQuery(new ColumnFamily(cf, StringSerializer.get(), StringSerializer.get()))
						.getKeyRange(null,null,'0','0',maxRows)
						.withColumnRange(new RangeBuilder().setMaxSize(maxColumns).build())
						.execute()
						.result.each{row ->

					out.println "    ${row.key} =>"
					row.columns.each {col ->
						out.println "        ${col.name} => '${col.stringValue}'"
					}
				}
				out.println""
			}
		}
	}

	/**
	 * Provides persistence methods for cassandra-orm plugin
	 */
	OrmPersistenceMethods orm = new AstyanaxPersistenceMethods()
}
