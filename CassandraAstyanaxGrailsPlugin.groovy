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
	def version = "0.0.SNAPSHOT"

	// the version or versions of Grails the plugin is designed for
	def grailsVersion = "1.3.7 > *"

	// the other plugins this plugin depends on
	def dependsOn = [:]

	// resources that are excluded from plugin packaging
	def pluginExcludes = [
			"grails-app/views/error.gsp"
	]

	def author = "Bob Florian"
	def authorEmail = "bob.florian@reachlocal.com"
	def title = "Astyanax Cassandra Client"
	def description = '''\\
This plugin exposes the Astyanax Cassandra client as a Grails service and adds dynamic methods to make using it from
Groovy more convenient.
'''

	// URL to the plugin's documentation
	def documentation = "http://grails.org/plugin/cassandra-astyanax"

	def doWithWebDescriptor = { xml ->
		// TODO Implement additions to web.xml (optional), this event occurs before
	}

	def doWithSpring = {
		// TODO Implement runtime spring config (optional)
	}

	def doWithDynamicMethods = { ctx ->
		// Dynamic methods to make Astyanax groovier
		AstyanaxDynamicMethods.addAll()
	}

	def doWithApplicationContext = { applicationContext ->
		// TODO Implement post initialization spring config (optional)
	}

	def onChange = { event ->
		// TODO Implement code that is executed when any artefact that this plugin is
		// watching is modified and reloaded. The event contains: event.source,
		// event.application, event.manager, event.ctx, and event.plugin.
	}

	def onConfigChange = { event ->
		// TODO Implement code that is executed when the project configuration changes.
		// The event is the same as for 'onChange'.
	}
}
