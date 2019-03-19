package com.redhat.demo.dm.ccfraud.drools;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class DroolsSessionFactory {

    protected static KieSession createDroolsSession(String sessionName) {
        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieContainer = kieServices.getKieClasspathContainer();
        return kieContainer.newKieSession(sessionName);
    }
}
