package com.dematic.labs.picketlink;

import org.picketlink.config.SecurityConfigurationBuilder;
import org.picketlink.event.SecurityConfigurationEvent;

import javax.enterprise.event.Observes;

@SuppressWarnings("UnusedDeclaration")
public class SecurityConfiguration {

    public void init(@Observes SecurityConfigurationEvent event) {
        SecurityConfigurationBuilder securityConfigurationBuilder = event.getBuilder();

        securityConfigurationBuilder
                .identity()
                .stateless(); // enable stateless authentication
    }
}
