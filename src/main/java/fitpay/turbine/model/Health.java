package fitpay.turbine.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Health {

    private boolean hystrix = false;

    public boolean isHystrix() {
        return hystrix;
    }

    public void setHystrix(boolean hystrix) {
        this.hystrix = hystrix;
    }

}
