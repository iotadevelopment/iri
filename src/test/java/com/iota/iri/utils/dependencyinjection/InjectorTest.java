package com.iota.iri.utils.dependencyinjection;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.conf.MainnetConfig;
import org.junit.Test;

import java.util.Set;

public class InjectorTest {
    private Set<Class> getInterfaces() {
        return null;
    }

    @Test
    public void testSomething() {
        Injector injector = new Injector();

        injector.registerInstance(new MainnetConfig());

        System.out.println(injector.getInstance(IotaConfig.class).getClass().getCanonicalName());

    }
}
