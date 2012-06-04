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
import org.springframework.beans.factory.InitializingBean
import com.netflix.astyanax.retry.RetryNTimes

/**
 * @author Bob Florian
 *
 */
class AstyanaxService implements InitializingBean
{
	boolean transactional = false

	def clusters = ConfigurationHolder.config.astyanax.clusters
	String defaultCluster = ConfigurationHolder.config?.astyanax?.defaultCluster ?: "standard"
	String defaultKeyspace = ConfigurationHolder.config?.astyanax?.defaultKeySpace ?: "AstyanaxTest"

	private clusterMap = [:]
	void afterPropertiesSet ()
	{
		clusters.each {key, props ->
			clusterMap[key] = [
					connectionPoolConfiguration:  new ConnectionPoolConfigurationImpl(props.connectionPoolName)
							.setPort(props.port)
							.setMaxConnsPerHost(props.maxConsPerHost)
							.setSeeds(props.seeds),

					contexts: [:]
			]
		}
	}

	/**
	 * Returns a keyspace entity
	 *
	 * @param name
	 * @return
	 */
	def keyspace(String name=defaultKeyspace, String cluster=defaultCluster)
	{
		context(name, cluster).entity
	}

	/**
	 * Constructs an Astyanax context and passed execution to a closure
	 *
	 * @param keyspace name of the keyspace
	 * @param block closure to be executed
	 * @throws Exception
	 */
	def withKeyspace(String keyspace=defaultKeyspace, String cluster=defaultCluster, Closure block) throws Exception
	{
		block(context(keyspace, cluster).entity)
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
	void showColumnFamilies (Collection names, String keyspace, String cluster=defaultCluster, Integer maxRows=50, Integer maxColumns=10, out=System.out) {
		names.each {String cf ->
			withKeyspace(keyspace) {ks ->
				out.println "${cf}:"
				ks.prepareQuery(new ColumnFamily(cf, StringSerializer.get(), StringSerializer.get()))
						.getKeyRange(null,null,'0','0',maxRows)
						.withColumnRange(new RangeBuilder().setMaxSize(maxColumns).build())
						.execute()
						.result.each{row ->

					out.println "    ${row.key} =>"
					row.columns.each {col ->
						try {
							out.println "        ${col.name} => '${col.stringValue}'"
						}
						catch (Exception ex) {
							out.println "        ${col.name} => ${col.longValue}"
						}
					}
				}
				out.println""
			}
		}
	}

	/**
	 * Provides persistence methods for cassandra-orm plugin
	 */
	def orm = new AstyanaxPersistenceMethods()
	
	def context(keyspace, cluster)
	{
		def context = clusterMap[cluster].contexts[keyspace]
		if (!context) {
			context = newContext(keyspace, cluster)
			context.start()
		}
		return context
	}
	
	private synchronized newContext(keyspace, cluster)
	{
		def entry = clusterMap[cluster]
		def context = entry.contexts[keyspace]
		if (!context) {
			def props = clusters[cluster]
			context = new AstyanaxContext.Builder()
					.forCluster(cluster)
					.forKeyspace(keyspace)
					.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
							.setDiscoveryType(props.discoveryType)
							.setRetryPolicy(props.retryPolicy)
					)
					.withConnectionPoolConfiguration(entry.connectionPoolConfiguration)
					.withConnectionPoolMonitor(props.connectionPoolMonitor)
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			entry.contexts[keyspace] = context
		}
		return context
	}
}
