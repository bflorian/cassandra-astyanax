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

package com.reachlocal.grails.plugins.cassandra.test.util

/**
 * @author: Bob Florian
 */
class TestSchema 
{
	def astyanaxService
	
	static initialize(astyanaxService)
	{
		if (!initialized) {
			createKeyspace(astyanaxService)
			initialized = true
		}
	}
	
    static private createKeyspace(astyanaxService)
	{
		runCqlScript("test/data/schema.txt")
	}

	static private runCqlScript(script) {
		def dsePath = System.getProperty('dsePath') ?: '/usr/local/dse'
		def cassandraCli = "$dsePath/bin/cassandra-cli -h localhost -f"

		def cmd = "$cassandraCli $script"
		def p = cmd.execute()
		def stderr = p.err?.text
		def stdout = p.text
		if (stderr) {
			System.err.println stderr
		} else {
			System.out.println stdout
		}
	}

	static private initialized = false
}
