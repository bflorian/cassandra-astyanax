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

import org.junit.Test
import static org.junit.Assert.*
import com.netflix.astyanax.connectionpool.OperationResult

/**
 * @author: Bob Florian
 */
class ExamplesTests 
{
	def astyanaxService

	@Test
	void testInsertingData()
	{
		def results = processFile("src/docs/ref/Examples/Inserting data.gdoc")
		results.each {k,v ->
			assertTrue v instanceof OperationResult
		}
	}

	@Test
	void testIncrementingCounterColumns()
	{
		def results = processFile("src/docs/ref/Examples/Incrementing counter columns.gdoc")
	}

	@Test
	void testQueryingForASingleColumn()
	{
		def results = processFile("src/docs/ref/Examples/Querying for a single column.gdoc")
		assertEquals "X", results["Standard Astyanax"]
	}

	@Test
	void testQueryingForASliceOfColumns()
	{
		def results = processFile("src/docs/ref/Examples/Querying for a slice of columns.gdoc")
	}

	@Test
	void testQueryingForAnEntireRow()
	{
		def results = processFile("src/docs/ref/Examples/Querying for an entire row.gdoc")
	}

	@Test
	void testQueryingForSpecificColumns()
	{
		def results = processFile("src/docs/ref/Examples/Querying for specific columns.gdoc")
	}

	@Test
	void testCountingTheNumberOfColumns()
	{
		def results = processFile("src/docs/ref/Examples/Counting the number of columns.gdoc")
	}

	@Test
	void testDeletingData()
	{
		def results = processFile("src/docs/ref/Examples/Deleting data.gdoc")
	}

	private processFile(filename)
	{
		def results = [:]
		def inCode = false
		def name = null
		def script = new StringBuffer()
		def scripts = [:]
		def file = new File(filename)
		file.eachLine{line ->
			def trim = line.trim()
			if (trim.startsWith("h2.")) {
				name = line[3..-1].trim()
			}
			else if (trim == "{code}") {
				if (inCode) {
					inCode = false
					scripts[name] = script.toString()
					script = new StringBuffer()
				}
				else {
					inCode = true
				}
			}
			else if (inCode) {
				script << line
				script << "\n"
			}
		}

		scripts.each {k, v ->
			def result = runScript(v)
			println "$k => $result"
			results[name] = result
		}
		return results
	}

	private runScript(script)
	{
		def fullScript = "import com.netflix.astyanax.util.*\n" + script
		def binding = new Binding(astyanaxService: astyanaxService)
		def shell = new GroovyShell(binding)
		shell.evaluate(fullScript)
	}
}
