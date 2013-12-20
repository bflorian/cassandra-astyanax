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

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.util.RangeBuilder
import org.springframework.beans.factory.InitializingBean
import com.netflix.astyanax.retry.RetryNTimes
import com.netflix.astyanax.connectionpool.NodeDiscoveryType

/**
 * @author Bob Florian
 *
 */
class AstyanaxService implements InitializingBean
{
	boolean transactional = false

	def grailsApplication

	def clusters
	String defaultCluster
	String defaultKeyspace

	private clusterMap = [:]

	/**
	 * Provides persistence methods for cassandra-orm plugin
	 */
	def orm

	/**
	 * Initializes all configured clusters
	 */
	void afterPropertiesSet ()
	{
		def config = grailsApplication.config
		orm = new AstyanaxPersistenceMethods(log: log)
		clusters = config.astyanax.clusters
		defaultCluster = config.astyanax.defaultCluster
		defaultKeyspace = clusters[defaultCluster].defaultKeyspace ?: config.astyanax.defaultKeyspace

		clusters.each {key, props ->
			def port = props.port ?: 9160
			def maxConsPerHost = props.maxConsPerHost ?: 10
			def connectionPoolName = props.connectionPoolName ?: key
			def connectTimeout = props.connectTimeout ?: 2000
			def socketTimeout = props.socketTimeout ?: 11000
			def maxTimeoutWhenExhausted = props.maxTimeoutWhenExhausted ?: 2000
			def maxTimeoutCount = props.maxTimeoutCount ?: 3
			def timeoutWindow = props.timeoutWindow ?: 10000

			ConnectionPoolConfiguration connectionPoolConfiguration =  new ConnectionPoolConfigurationImpl(connectionPoolName)
				.setPort(port)
				.setMaxConnsPerHost(maxConsPerHost)
				.setConnectTimeout(connectTimeout)
				.setSocketTimeout(socketTimeout)
				.setMaxTimeoutWhenExhausted(maxTimeoutWhenExhausted)
				.setMaxTimeoutCount(maxTimeoutCount)
				.setTimeoutWindow(timeoutWindow)
				.setSeeds(props.seeds)

			if (props.username && props.password) {
				connectionPoolConfiguration.authenticationCredentials = new SimpleAuthenticationCredentials(props.username, props.password)
			}

			clusterMap[key] = [
				connectionPoolConfiguration: connectionPoolConfiguration,
				contexts: [:],
				defaultKeyspace: props.defaultKeyspace ?: config.astyanax.defaultKeyspace
			]
		}
	}

	def defaultKeyspaceName(cluster)
	{
		clusterMap[cluster].defaultKeyspace
	}

	/**
	 * Returns a keyspace entity
	 *
	 * @param name Optional, ame of the keyspace, defaults to configured defaultKeyspace
	 * @param cluster Optional, name of the Cassandra cluster, defaults to configured defaultCluster
	 *
	 */
	def keyspace(String name=null, String cluster=defaultCluster)
	{
		context(name, cluster).entity
	}

	/**
	 * Constructs an Astyanax context and passed execution to a closure
	 *
	 * @param name Optional, ame of the keyspace, defaults to configured defaultKeyspace
	 * @param cluster Optional, name of the Cassandra cluster, defaults to configured defaultCluster
	 *
	 */
	def withKeyspace(String keyspace=null, String cluster=defaultCluster, Closure block) throws Exception
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
			withKeyspace(keyspace, cluster) {ks ->
				def cfd = ks.describeKeyspace().getColumnFamily(cf)
				out.println "${cf}:"
				ks.prepareQuery(new ColumnFamily(cf, StringSerializer.get(), StringSerializer.get()))
						.getKeyRange(null,null,'0','0',maxRows)
						.withColumnRange(new RangeBuilder().setMaxSize(maxColumns).build())
						.execute()
						.result.each{row ->

					out.println "    ${rowKey(row,cfd)} =>"
					row.columns.each {col ->
						out.println "        ${columnName(col, cfd)} => ${columnValue(col, cfd)}"
					}
				}
				out.println""
			}
		}
	}

	private rowKey(row, cf) {
		def vc = cf.keyValidationClass
		if (dataType(vc) in ["UUID","TimeUUID"]) {
			UUID.fromBytes(row.rawKey.array()).toString()
		}
		else {
			row.key
		}
	}
	private columnName(col, cf) {
		def ct = cf.comparatorType
		if (dataType(ct) in ["UUID","TimeUUID"]) {
			UUID.fromBytes(col.rawName.array()).toString()
		}
		else {
			col.name
		}
	}

	private columnValue(col, cf) {
		def cdl = cf.columnDefinitionList
		def cd = cdl.find{it.name == col.name}
		def vc = cd?.validationClass ?: cf.defaultValidationClass
		if (vc) {
			def type = dataType(vc)
			switch(type) {
				case "UUID":
				case "TimeUUID":
					return col.UUIDValue
				case "Long":
				case "CounterColumn":
					return col.longValue
				case "Boolean":
					return col.booleanValue
				case "Date":
					return col.dateValue
				default:
					return "'$col.stringValue'"
			}
		}
		else {
			return "'$col.stringValue'"
		}
	}

	private static dataType(String s) {
		final pat = ~/.*\.([a-z,A-Z,0-9]+)Type\)?$/
		pat.matcher(s).replaceAll('$1')
	}


	/**
	 * Finds or creates an Astyanax context for the specified cluster and keyspace
	 *
	 * @param keyspace name of the keyspace
	 * @param cluster name of the cluster
	 *
	 */
	def context(keyspace, cluster)
	{
		def ks = keyspace ?: clusterMap[cluster].defaultKeyspace
		def context = clusterMap[cluster].contexts[ks]
		if (!context) {
			context = newContext(ks, cluster)
			context.start()
		}
		return context
	}

	/**
	 * Constructs new context and stores it in the map
	 */
	private synchronized newContext(keyspace, cluster)
	{
		def entry = clusterMap[cluster]
		def context = entry.contexts[keyspace]
		if (!context) {

			def props = clusters[cluster]
			//Default the pool type to the same default astyanax uses Round Robin.
			ConnectionPoolType connectionPoolType = props.connectionPoolType ?: ConnectionPoolType.ROUND_ROBIN
			def connectionPoolMonitor = props.connectionPoolMonitor ?: new Slf4jConnectionPoolMonitorImpl()
			def discoveryType = props.discoveryType ?:  NodeDiscoveryType.NONE
			def retryCount = props.retryCount ?: 3
			def retryPolicy = props.retryPolicy ?: new RetryNTimes(retryCount)


			def configuration = new AstyanaxConfigurationImpl()
					.setConnectionPoolType(connectionPoolType)
					.setDiscoveryType(discoveryType)
					.setRetryPolicy(retryPolicy)

			if (props.defaultWriteConsistencyLevel) {
				configuration.setDefaultWriteConsistencyLevel(props.defaultWriteConsistencyLevel)
			}

			if (props.defaultReadConsistencyLevel) {
				configuration.setDefaultReadConsistencyLevel(props.defaultReadConsistencyLevel)
			}

			if (props.cassandraVersion) {
				configuration.setTargetCassandraVersion(props.cassandraVersion)
			}

			if (props.cqlVersion) {
			    configuration.setCqlVersion(props.cqlVersion)
			}

			context = new AstyanaxContext.Builder()
					.forCluster(cluster)
					.forKeyspace(keyspace)
					.withAstyanaxConfiguration(configuration)
					.withConnectionPoolConfiguration(entry.connectionPoolConfiguration)
					.withConnectionPoolMonitor(connectionPoolMonitor)
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			entry.contexts[keyspace] = context
		}
		return context
	}
}
