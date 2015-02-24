/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.dematic.labs.http.picketlink;

import com.dematic.labs.business.picketlink.idm.credential.TokenConsumer;
import com.dematic.labs.business.picketlink.idm.credential.handler.SignatureTokenCredentialHandler;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import org.picketlink.config.SecurityConfigurationBuilder;
import org.picketlink.event.SecurityConfigurationEvent;

import javax.enterprise.event.Observes;

/**
 * <p>A simple CDI observer for the {@link org.picketlink.event.SecurityConfigurationEvent}.</p>
 *
 * <p>The event is fired during application startup and allows you to provide any configuration to PicketLink
 * before it is initialized.</p>
 *
 * <p>All the configuration related with Http Security is provided from this bean.</p>
 *
 * @author Pedro Igor
 */
@SuppressWarnings("UnusedDeclaration")
public class HttpSecurityConfiguration {

    public void onInit(@Observes SecurityConfigurationEvent event) {
        SecurityConfigurationBuilder builder = event.getBuilder();

//        builder
//            .http()
//                .forPath("/protected/*")
//                    .authenticateWith()
//                        .form()
//                            .loginPage("/login.jsf")
//                            .errorPage("/error.jsf")
//                            .restoreOriginalRequest()
//                .forPath("/logout")
//                    .logout()
//                    .redirectTo("/index.html");

        builder
            .http()
                .forPath("/protected/*")
                    .authorizeWith()
                        .expression("#{identity.account.partition.name}")
                    .redirectTo("/error.jsf").whenForbidden()
                .forPath("/logout")
                    .logout()
                    .redirectTo("/index.html")
                .forPath("/resources/*")
                    .authenticateWith().scheme(DLabsAuthenticationScheme.class)
            .idmConfig()
                .named("token.config")
                    .stores()
                .token()
                .addCredentialHandler(SignatureTokenCredentialHandler.class)
                        //                        .add(SignatureTokenStoreConfiguration.class, SignatureTokenStoreConfigurationBuilder.class)
                .tokenConsumer(new TokenConsumer())
                .supportAllFeatures()

                .named("jpa.config")
                .stores()
                    .jpa()
                        // Specify that this identity store configuration supports all features
                        .supportAllFeatures()
                        .addCredentialHandler(SignatureTokenCredentialHandler.class)
            ;
    }

}
