/*
 * Copyright (c) 2015, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.jersey.tests.integration.multimodule.ejb.web1;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriInfo;

import javax.ejb.EJB;
import javax.ejb.Stateless;

import org.glassfish.jersey.tests.integration.multimodule.ejb.lib.EjbCounterResource;

/**
 * JAX-RS resource backed by a stateless EJB bean placed in WAR module.
 *
 * @author Jakub Podlesak (jakub.podlesak at oracle.com)
 * @author Libor Kramolis (libor.kramolis at oracle.com)
 */
@Stateless
@Path("war-stateless")
public class WarStatelessResource {

    @EJB EjbCounterResource counter;
    @Context UriInfo uriInfo;

    @GET
    public int getCount() {
        return counter.getCount();
    }

    @GET
    @Path("{uriInfo}")
    public String getPath() {
        return uriInfo != null ? uriInfo.getPath() : "uri info is null";
    }
}
