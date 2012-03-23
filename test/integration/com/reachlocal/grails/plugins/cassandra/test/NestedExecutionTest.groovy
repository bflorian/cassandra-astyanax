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

/**
 * @author: Bob Florian
 */
class NestedExecutionTest extends GroovyTestCase
{
	def astyanaxService

	void testTandem()
	{
		astyanaxService.execute() {ks1 ->
		}
		astyanaxService.execute() {ks2 ->
		}
	}

	void testTwoLevel()
	{
		astyanaxService.execute() {ks1 ->
			astyanaxService.execute() {ks2 ->

			}
		}
	}

	void testTwoThreads()
	{
		Thread.start {
			astyanaxService.execute() {ks1 ->
				println "thread 1"
				Thread.sleep(100)
				println "thread 1 ending"
			}
		}
		Thread.start {
			astyanaxService.execute() {ks2 ->
				println "thread 2"
				Thread.sleep(100)
				println "thread 2 ending"
			}
		}
	}
}
