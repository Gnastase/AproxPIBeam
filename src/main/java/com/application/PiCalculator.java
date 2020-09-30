package com.application;

import java.io.Serializable;
import java.util.logging.Logger;

public class PiCalculator implements Serializable {

    private static final Logger __logger = Logger.getLogger("com.application.ApproxPIBeam");

    public static String calculatePI(KVWrapper input,Long total){
        Double approxPI = 4.0 * input.value.getValue() / (total * 1.0);
        Double  relError =  Math.abs(approxPI - Math.PI) / Math.PI;
        String result = String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", approxPI, Math.PI,100 * relError);
        __logger.info(result);
        return  result;
    }

}
