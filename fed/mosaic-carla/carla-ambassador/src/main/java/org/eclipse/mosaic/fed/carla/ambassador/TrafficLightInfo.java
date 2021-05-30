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
package org.eclipse.mosaic.fed.carla.ambassador;

import org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightState;

import java.util.ArrayList;
import java.util.List;

public class TrafficLightInfo {

    private final int poleIndex;
    private final int numberOfStates;
    private List<TrafficLightState> trafficLightStates = new ArrayList<>();

    public TrafficLightInfo(int poleIndex, int numberOfStates) {
        this.poleIndex = poleIndex;
        this.numberOfStates = numberOfStates;
        setTrafficLightStates(new TrafficLightState(true, false, false));
    }

    public void setTrafficLightStates(TrafficLightState trafficLightState) {
        trafficLightStates.clear();
        for (int i = 0; i < numberOfStates; i++) {
            this.trafficLightStates.add(trafficLightState);
        }
    }

    public List<TrafficLightState> getTrafficLightStates() {
        return trafficLightStates;
    }
}
