package com.iota.iri.utils.dependencyinjection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Injector {
    private Map<Class<?>, Object> instances = new HashMap<>();

    public Injector(Object ... instancesToRegister) {
        for(Object instanceToRegister : instancesToRegister) {
            registerInstance(instanceToRegister);
        }
    }

    public static Set<Class<?>> getInheritance(Class<?> in) {
        Set<Class<?>> result = new HashSet<>();

        result.add(in);
        getInheritance(in, result);

        return result;
    }

    /**
     * Get inheritance of type.
     *
     * @param in
     * @param result
     */
    private static void getInheritance(Class<?> in, Set<Class<?>> result) {
        Class<?> superclass = getSuperclass(in);

        if(superclass != null) {
            result.add(superclass);
            getInheritance(superclass, result);
        }

        getInterfaceInheritance(in, result);
    }

    /**
     * Get interfaces that the type inherits from.
     *
     * @param in
     * @param result
     */
    private static void getInterfaceInheritance(Class<?> in, Set<Class<?>> result) {
        for(Class<?> c : in.getInterfaces()) {
            result.add(c);

            getInterfaceInheritance(c, result);
        }
    }

    /**
     * Get superclass of class.
     *
     * @param in
     * @return
     */
    private static Class<?> getSuperclass(Class<?> in) {
        if(in == null) {
            return null;
        }

        if(in.isArray() && in != Object[].class) {
            Class<?> type = in.getComponentType();

            while(type.isArray()) {
                type = type.getComponentType();
            }

            return type;
        }

        return in.getSuperclass();
    }

    public void registerInstance(Object instance) {
        Set<Class<?>> allTypes = getInheritance(instance.getClass());

        allTypes.remove(Object.class);

        for(Class clazz: allTypes) {
            instances.put(clazz, instance);
        }
    }

    public void registerInstance(Class<?> clazz, Object instance) {
        instances.put(clazz, instance);
    }

    public <CLASS> CLASS getInstance(Class<CLASS> clazz) {
        try {
            CLASS managedInstance = (CLASS) instances.get(clazz);
            if (managedInstance == null) {
                managedInstance = (CLASS) clazz.getMethod("create", Injector.class).invoke(null, this);

                instances.put(clazz, managedInstance);
            }

            return managedInstance;
        } catch (Exception e) {
            System.out.println("CRITICAL ERROR IN DEPENDENCY INJECTION OF " + clazz.getCanonicalName());

            e.printStackTrace();

            System.exit(0);

            return null;
        }
    }
}
