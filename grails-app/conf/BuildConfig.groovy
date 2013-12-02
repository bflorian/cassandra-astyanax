grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"
//grails.project.war.file = "target/${appName}-${appVersion}.war"
grails.project.dependency.resolution = {
	// inherit Grails' default dependencies
	inherits("global") {
		// uncomment to disable ehcache
		// excludes 'ehcache'
	}
	log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'
	repositories {
		grailsPlugins()
		grailsHome()
		mavenCentral()
	}
	dependencies {
		// specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.

		runtime 'com.github.stephenc.high-scale-lib:high-scale-lib:1.1.1'
		runtime 'com.github.stephenc.eaio-uuid:uuid:3.2.0'

		compile('com.netflix.astyanax:astyanax-core:1.56.44') {
			excludes 'slf4j-log4j12', 'junit', 'commons-logging'
		}
		compile('com.netflix.astyanax:astyanax-thrift:1.56.44') {
			excludes 'slf4j-log4j12', 'junit', 'commons-logging'
		}
		compile('com.netflix.astyanax:astyanax-cassandra:1.56.44') {
			excludes 'slf4j-log4j12', 'junit', 'commons-logging'
		}

		/*
		| Error SLF4J: Class path contains multiple SLF4J bindings.
		| Error SLF4J: Found binding in [jar:file:/Users/bflorian/.grails/ivy-cache/org.grails/grails-plugin-log4j/jars/grails-plugin-log4j-2.2.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
		| Error SLF4J: Found binding in [jar:file:/Users/bflorian/.grails/ivy-cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
		| Error SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
		 */

		//compile('com.netflix.astyanax:astyanax:1.0.3') {
		//	excludes 'slf4j-log4j12', 'junit', 'commons-logging'
		//}

		test "org.spockframework:spock-grails-support:0.7-groovy-2.0"

	}

	plugins {

		build(":tomcat:$grailsVersion",
			":release:2.2.1",
			":rest-client-builder:1.0.3") {
			export = false
		}
		test(":spock:0.7",
	  ":code-coverage:1.2.6") {
			export = false
			exclude "spock-grails-support"
		}

	}
}



