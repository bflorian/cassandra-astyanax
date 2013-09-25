import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer

// configuration for plugin testing - will not be included in the plugin zip

astyanax {
	clusters {
		standard {
			seeds = ["localhost:9160"]
			defaultKeyspace = "AstyanaxTest"
			columnFamilies {
				AstyanaxTest {
					LongColumnCF = new ColumnFamily("LongColumnCF", StringSerializer.get(), LongSerializer.get())
				}
			}
		}
		dummy1 {
			seeds = ["localhost:9160"]
			defaultKeyspace = "Dummy1Default"
		}
		dummy2 {
			seeds = ["localhost:9160"]
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

astyanax.clusters.standard.connectTimeout = 3000
astyanax.clusters.standard.socketTimeout = 25000
astyanax.clusters.standard.retryCount = 5

