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

public class TrafficLightPole {

    private final int poleIndex;
    private final int numberOfStates;
    private final boolean strictConversion;
    private List<TrafficLightState> mosaicStates = new ArrayList<>();
    private String carlaState;

    /**
     * Possible Carla traffic light states.
     */
    private final String TL_CARLA_RED = "r";
    private final String TL_CARLA_YELLOW = "y";
    private final String TL_CARLA_GREEN = "G";
    private final String TL_CARLA_OFF = "0";

    /**
     * Defined vehicle signals bits by Sumo (https://sumo.dlr.de/docs/TraCI/Vehicle_Signalling.html)
     */
    private final int VEH_SIGNAL_BLINKER_RIGHT = 0;
    private final int VEH_SIGNAL_BLINKER_LEFT = 1;
    private final int VEH_SIGNAL_BLINKER_EMERGENCY = 2;
    private final int VEH_SIGNAL_BRAKELIGHT = 3;
    private final int VEH_SIGNAL_BACKDRIVE = 7;

    /**
     * Possible Mosaic traffic light states.
     */
    TrafficLightState TL_MOSAIC_RED = new TrafficLightState(true, false, false);
    TrafficLightState TL_MOSAIC_YELLOW = new TrafficLightState(false, false, true);
    TrafficLightState TL_MOSAIC_RED_YELLOW = new TrafficLightState(true, false, true);
    TrafficLightState TL_MOSAIC_GREEN = new TrafficLightState(false, true, false);
    TrafficLightState TL_MOSAIC_OFF = new TrafficLightState(false, false, false);

    /**
     * Constructor. Traffic light pole is initialized with "off" state.
     * @param poleIndex Index of the pole.
     * @param numberOfStates Number of traffic light states the pole has.
     * @param strictConversion Translation from the more complex Mosaic traffic light state representation can be
     *                         strict or not strict:<br>
     *                         Strict conversion: if any state is red or red + yellow -> red; if no state is red but at
     *                         least one is yellow -> yellow; if all states are green or green with priority -> green.<br>
     *                         Not strict conversion: if any state is green or green with priority -> green; if no state
     *                         is green but at least one is yellow -> yellow; if all states are red or red + yellow ->
     *                         red.
     */
    public TrafficLightPole(int poleIndex, int numberOfStates, boolean strictConversion) {
        this.poleIndex = poleIndex;
        this.numberOfStates = numberOfStates;
        setMosaicStates(TL_MOSAIC_OFF);
        carlaState = TL_CARLA_OFF;
        this.strictConversion = strictConversion;
    }

    /**
     * Sets the state of the traffic light based on a Carla traffic light state string.
     * @param state Carla traffic light state.
     */
    public void setState(String state) {
        carlaState = state;
        switch (state) {
            case TL_CARLA_RED:
                setMosaicStates(TL_MOSAIC_RED);
                break;
            case TL_CARLA_GREEN:
                setMosaicStates(TL_MOSAIC_GREEN);
                break;
            case TL_CARLA_YELLOW:
                setMosaicStates(TL_MOSAIC_YELLOW);
                break;
            case TL_CARLA_OFF:
            default:
                setMosaicStates(TL_MOSAIC_OFF);
        }
    }

    /**
     * Sets the state of the traffic light based on a list of Mosaic {@link TrafficLightState}s.
     * @param states List of Mosaic {@link TrafficLightState}s.
     */
    public void setState(List<TrafficLightState> states) {
        mosaicStates = states;
        setCarlaState(states);
    }

    /**
     * Multiplies the traffic light state according to the number of states the pole has.
     * @param trafficLightState Traffic light state.
     */
    private void setMosaicStates(TrafficLightState trafficLightState) {
        mosaicStates.clear();
        for (int i = 0; i < numberOfStates; i++) {
            this.mosaicStates.add(trafficLightState);
        }
    }

    /**
     * Reduces the more complex traffic light states representation of Mosaic to the less complex one of Carla.
     * @param states Mosaic traffic light states.
     */
    private void setCarlaState(List<TrafficLightState> states) {
        // see constructor for description of strict conversion
        if (strictConversion) {
            if (states.contains(TL_MOSAIC_GREEN)) {
                carlaState = TL_CARLA_GREEN;
            }
            if (states.contains(TL_MOSAIC_YELLOW)) {
                carlaState = TL_CARLA_YELLOW;
            }
            if (states.contains(TL_MOSAIC_RED) || states.contains(TL_MOSAIC_RED_YELLOW)) {
                carlaState = TL_CARLA_RED;
            }
        } else {
            if (states.contains(TL_MOSAIC_RED) || states.contains(TL_MOSAIC_RED_YELLOW)) {
                carlaState = TL_CARLA_RED;
            }
            if (states.contains(TL_MOSAIC_YELLOW)) {
                carlaState = TL_CARLA_YELLOW;
            }
            if (states.contains(TL_MOSAIC_GREEN)) {
                carlaState = TL_CARLA_GREEN;
            }
        }
    }

    public List<TrafficLightState> getMosaicStates() {
        return mosaicStates;
    }

    public String getCarlaState() {
        return carlaState;
    }

    public int getPoleIndex() {
        return poleIndex;
    }

    public int getNumberOfStates() {
        return numberOfStates;
    }
}
