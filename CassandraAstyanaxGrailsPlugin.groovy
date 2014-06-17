import com.reachlocal.grails.plugins.cassandra.astyanax.AstyanaxDynamicMethods

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

class CassandraAstyanaxGrailsPlugin
{
	// the plugin version
	def version = "1.0.8-SNAPSHOT"

	// the version or versions of Grails the plugin is designed for
	def grailsVersion = "2.0.0 > *"

	// the other plugins this plugin depends on
	def dependsOn = [:]

	// resources that are excluded from plugin packaging
	def pluginExcludes = [
			"grails-app/views/error.gsp",
			'src/docs/**'
	]

	def author = "Bob Florian"
	def authorEmail = "bob.florian@reachlocal.com"
	def title = "Astyanax Cassandra Client"
	def license = 'APACHE'
	def organization = [name: 'ReachLocal', url: 'http://www.reachlocal.com/']
	def issueManagement = [system: 'JIRA', url: 'http://jira.grails.org/browse/GPCASSANDRAASTYANAX']
	def scm = [url: 'https://github.com/bflorian/cassandra-astyanax']

	def description = '''This plugin exposes the Astyanax Cassandra client as a Grails service and adds dynamic methods
to make using it from Groovy more convenient.  It also implements the interface defined by the cassandra-orm plugin
that provides GORM-like dynamic methods for storing Groovy objects and relationships in Cassandra.
Note that this plugin does not implement the GORM API.
'''
	// URL to the plugin's documentation
	def documentation = "http://bflorian.github.io/cassandra-astyanax/"

	def doWithDynamicMethods = { ctx ->
		// Dynamic methods to make Astyanax groovier
		AstyanaxDynamicMethods.addAll()
	}
}
