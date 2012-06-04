// configuration for plugin testing - will not be included in the plugin zip

astyanax {
	clusters {
		standard {
			name = "Test Cluster"
			seeds = ["localhost:9160"]
			port = 9160
			maxConsPerHost = 10
			connectionPoolName = "MyConnectionPool"
			connectionPoolMonitor = new com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor()
			discoveryType = com.netflix.astyanax.connectionpool.NodeDiscoveryType.NONE
			retryPolicy = new com.netflix.astyanax.retry.RetryNTimes(3)
			defaultKeyspace = "AstyanaxTest"
		}
	}
	defaultCluster = 'standard'
}

log4j = {
    // Example of changing the log pattern for the default console
    // appender:
    //
    //appenders {
    //    console name:'stdout', layout:pattern(conversionPattern: '%c{2} %m%n')
    //}

    error  'org.codehaus.groovy.grails.web.servlet',  //  controllers
           'org.codehaus.groovy.grails.web.pages', //  GSP
           'org.codehaus.groovy.grails.web.sitemesh', //  layouts
           'org.codehaus.groovy.grails.web.mapping.filter', // URL mapping
           'org.codehaus.groovy.grails.web.mapping', // URL mapping
           'org.codehaus.groovy.grails.commons', // core / classloading
           'org.codehaus.groovy.grails.plugins', // plugins
           'org.codehaus.groovy.grails.orm.hibernate', // hibernate integration
           'org.springframework',
           'org.hibernate',
           'net.sf.ehcache.hibernate'

    warn   'org.mortbay.log'
}
grails.views.default.codec="none" // none, html, base64
grails.views.gsp.encoding="UTF-8"
