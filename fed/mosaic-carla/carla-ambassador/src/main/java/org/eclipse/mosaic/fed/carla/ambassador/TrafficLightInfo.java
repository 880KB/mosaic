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
