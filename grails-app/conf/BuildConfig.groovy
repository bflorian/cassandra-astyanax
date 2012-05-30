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
        grailsCentral()

        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        //mavenLocal()
        mavenCentral()
        //mavenRepo "http://snapshots.repository.codehaus.org"
        //mavenRepo "http://repository.codehaus.org"
        //mavenRepo "http://download.java.net/maven/2/"
        //mavenRepo "http://repository.jboss.com/maven2/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.

        // runtime 'mysql:mysql-connector-java:5.1.13'
		//runtime 'joda-time:joda-time:2.0'
		//runtime 'org.apache.servicemix.bundles:org.apache.servicemix.bundles.commons-csv:1.0-r706900_3'
		runtime 'com.github.stephenc.high-scale-lib:high-scale-lib:1.1.1'
		runtime 'com.google.guava:guava:11.0.2'
		runtime 'com.github.stephenc.eaio-uuid:uuid:3.2.0'
		compile ('com.netflix.astyanax:astyanax:1.0.3') {
			excludes 'slf4j-log4j12'
		}

    }
}

grails.project.repos.beanstalkRepository.url = "https://mural.svn.beanstalkapp.com/grails-plugins"
grails.project.repos.beanstalkRepository.type = "svn"

grails.project.repos.dreamhostRepository.url = "http://cm.florian.org/grailsplugins"
grails.project.repos.dreamhostRepository.type = "svn"

grails.project.repos.default = "beanstalkRepository"
grails.release.scm.enabled = false

