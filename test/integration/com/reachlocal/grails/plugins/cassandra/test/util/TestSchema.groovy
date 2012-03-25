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
		def cql = astyanaxService.cql()
		
		new File("test/data/schema.cql").eachLine {line ->
			try {
				cql.execute(line)
			}
			catch (Exception e) {
				println "${line} -- ${e}"
			}
		}
}
	
	static private initialized = false
}
