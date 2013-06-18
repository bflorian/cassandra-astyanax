import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer

// configuration for plugin testing - will not be included in the plugin zip

astyanax {
	clusters {
		standard {
			seeds = "10.0.10.60:9160,10.0.10.61:9160,10.0.10.62:9160"
			defaultKeyspace = "AstyanaxTest"
			connectionPoolType = com.netflix.astyanax.connectionpool.impl.ConnectionPoolType.TOKEN_AWARE
			discoveryType = com.netflix.astyanax.connectionpool.NodeDiscoveryType.RING_DESCRIBE

			columnFamilies {
				AstyanaxTest {
					LongColumnCF = new ColumnFamily("LongColumnCF", StringSerializer.get(), LongSerializer.get())
				}
			}
		}
		dummy1 {
			connectionPoolType = com.netflix.astyanax.connectionpool.impl.ConnectionPoolType.TOKEN_AWARE
			discoveryType = com.netflix.astyanax.connectionpool.NodeDiscoveryType.RING_DESCRIBE
			seeds = "10.0.10.60:9160,10.0.10.61:9160,10.0.10.62:9160"
			defaultKeyspace = "Dummy1Default"
		}
		dummy2 {
			connectionPoolType = com.netflix.astyanax.connectionpool.impl.ConnectionPoolType.TOKEN_AWARE
			discoveryType = com.netflix.astyanax.connectionpool.NodeDiscoveryType.RING_DESCRIBE
			seeds = "10.0.10.60:9160,10.0.10.61:9160,10.0.10.62:9160"
		}
	}
	defaultKeyspace = "OverallDefault"
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
