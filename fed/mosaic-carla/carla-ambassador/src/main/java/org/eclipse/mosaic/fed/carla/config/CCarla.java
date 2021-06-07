/*
 * Copyright (c) 2020 Fraunhofer FOKUS and others. All rights reserved.
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contact: mosaic@fokus.fraunhofer.de
 */

package org.eclipse.mosaic.fed.carla.config;

import com.google.gson.annotations.JsonAdapter;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;

import java.io.Serializable;

public class CCarla implements Serializable {

    private static final long serialVersionUID = 6086738641932706254L;

    /**
     * The Interval after which positions are published.
     * Define the size of one simulation step in Carla (minimal value: 100).
     * The default value is 1000 (1s). Unit: [ms].
     */
    @JsonAdapter(TimeFieldAdapter.LegacyMilliSeconds.class)
    public Long updateInterval = 1000L;

    /**
     * Defines who the traffic light manager is. Possible values: "mosaic" or "carla".
     */
    public String tlsManager = "mosaic";

    /**
     * Toggles the translation from the more complex Mosaic traffic light state representation.
     * See {@link org.eclipse.mosaic.fed.carla.ambassador.TrafficLightPole}
     */
    public boolean tlsStrictConversion = true;

    /**
     * Path to the Carla co-simulation folder.
     */
    public String pathToCarlaCoSimulation = "";

    public boolean carlaIsTlsManager() {
        return tlsManager.equals("carla");
    }

    public boolean mosaicIsTlsManager() {
        return tlsManager.equals("mosaic");
    }
}
