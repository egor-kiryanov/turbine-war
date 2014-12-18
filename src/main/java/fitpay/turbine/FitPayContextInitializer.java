package fitpay.turbine;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.init.TurbineInit;

public class FitPayContextInitializer implements ServletContextListener {
	private static Logger log = LoggerFactory.getLogger(FitPayContextInitializer.class);
	
	public void contextDestroyed(ServletContextEvent sce) {
		log.info("stopping turbine");
		TurbineInit.stop();
	}

	public void contextInitialized(ServletContextEvent sce) {
		log.info("starting turbine");
		TurbineInit.init();
	}

}
