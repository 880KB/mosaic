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

import com.google.common.collect.Lists;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.eclipse.mosaic.fed.carla.grpc.*;
import org.eclipse.mosaic.fed.carla.grpcstubs.CarlaGrpcClient;
import org.eclipse.mosaic.interactions.mapping.VehicleRegistration;
import org.eclipse.mosaic.interactions.mapping.advanced.ScenarioTrafficLightRegistration;
import org.eclipse.mosaic.interactions.traffic.*;
import org.eclipse.mosaic.interactions.vehicle.VehicleFederateAssignment;
import org.eclipse.mosaic.interactions.vehicle.VehicleRouteRegistration;
import org.eclipse.mosaic.lib.enums.DriveDirection;
import org.eclipse.mosaic.lib.enums.VehicleClass;
import org.eclipse.mosaic.lib.geo.CartesianPoint;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightGroupInfo;
import org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightState;
import org.eclipse.mosaic.lib.objects.vehicle.*;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.rti.api.AbstractFederateAmbassador;
import org.eclipse.mosaic.rti.api.IllegalValueException;
import org.eclipse.mosaic.rti.api.Interaction;
import org.eclipse.mosaic.rti.api.InternalFederateException;
import org.eclipse.mosaic.rti.api.parameters.AmbassadorParameter;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CarlaAmbassador extends AbstractFederateAmbassador {
    /**
     * Channel to connect to grpc-client
     */
    Channel channel;

    /**
     * Client for grpc connection
     */
    CarlaGrpcClient client;

    /**
     * Simulation time at which the positions are published next.
     */
    long nextTimeStep;

    /**
     * List of {@link Interaction}s which will be cached till a time advance occurs.
     */
    private final List<Interaction> interactionList = new ArrayList<>();

    /**
     * Group name of all Carla controlled vehicles. Is used during vehicle registration.
     */
    private final String vehicleGroup = "carla-controlled";

    /**
     * Map of vehicles which were registered via the VehicleRegistration interaction (without Carla vehicles).
     */
    private HashMap<String, VehicleType> registeredVehicles = new HashMap<>();

    /**
     * Map of vehicle types which were registered via the VehicleTypesInitialization interaction.
     */
    private HashMap<String, VehicleType> registeredVehicleTypes = new HashMap<>();

    /**
     * defined vehicle signals bits by Sumo (https://sumo.dlr.de/docs/TraCI/Vehicle_Signalling.html)
     */
    private final int VEH_SIGNAL_BLINKER_RIGHT = 0;
    private final int VEH_SIGNAL_BLINKER_LEFT = 1;
    private final int VEH_SIGNAL_BLINKER_EMERGENCY = 2;
    private final int VEH_SIGNAL_BRAKELIGHT = 3;
    private final int VEH_SIGNAL_BACKDRIVE = 7;

    /**
     * Possible traffic light states.
     */
    TrafficLightState TL_RED = new TrafficLightState(true, false, false);
    TrafficLightState TL_YELLOW = new TrafficLightState(false, false, true);
    TrafficLightState TL_GREEN = new TrafficLightState(false, true, false);

    /**
     * List of all registered traffic light group IDs to the rti and the according Carla landmark ids of the traffic
     * lights belonging to this group. (rti group ID -> Carla landmark IDs)
     */
    private HashMap<String, List<String>> registeredTrafficLightGroupIds = new HashMap<>();

    /**
     * Holds information about a Carla traffic light. (Carla landmark ID -> traffic light info)
     */
    private HashMap<String, TrafficLightInfo> carlaTrafficLightsInfo = new HashMap<>();

    public CarlaAmbassador(AmbassadorParameter ambassadorParameter) {
        super(ambassadorParameter);
        log.info("Carla Ambassador successful started!");
    }

    /**
     * This method is called to tell the federate the start time and the end time.
     *
     * @param startTime Start time of the simulation run in nano seconds.
     * @param endTime   End time of the simulation run in nano seconds.
     * @throws InternalFederateException Exception is thrown if an error is occurred while execute of a federate.
     */
    @Override
    public void initialize(long startTime, long endTime) throws InternalFederateException {
        super.initialize(startTime, endTime);

        nextTimeStep = startTime;

        connectToGrpc();

        try {
            rti.requestAdvanceTime(nextTimeStep, 0, (byte) 1);
        } catch (IllegalValueException e) {
            log.error("Error during advanceTime request", e);
            throw new InternalFederateException(e);
        }
    }

    /**
     * Creates the grpc-client and tries connects it with the grpc-carla-server
     */
    private void connectToGrpc() {
        log.warn("Trying to connect tp GRPC Server (Carla)");
        System.out.println("Trying to connect to GRPC Server (Carla)");
        String target = "localhost:50051";
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        client = new CarlaGrpcClient(channel);
    }

    /**
     * This method processes the interactions.
     *
     * @param interaction The interaction that can be processed
     * @throws InternalFederateException Exception is thrown if an error is occurred while execute of a federate.
     */
    @Override
    public void processInteraction(Interaction interaction) throws InternalFederateException {

        log.debug("Got new interaction {} with time {} ns", interaction.getTypeId(), interaction.getTime());

            // ... everything is saved for later
            interactionList.add(interaction);
    }

    /**
     * This processes all other types of interactions as part of {@link #processTimeAdvanceGrant}.
     *
     * @param interaction The interaction to process.
     * @param time        The time of the processed interaction.
     * @throws InternalFederateException Exception if the interaction time is not correct.
     */
    protected void processInteractionAdvanced(Interaction interaction, long time) throws InternalFederateException {
        // make sure the interaction is not in the future
        if (interaction.getTime() > time) {
            throw new InternalFederateException("Interaction time lies in the future:" + interaction.getTime() + ", current time:" + time);
        }

        // only process interactions not sent by CarlaAmbassador
        if (!interaction.getSenderId().equals(getId())) {
            if (interaction.getTypeId().equals(VehicleUpdates.TYPE_ID)) {
                receiveInteraction((VehicleUpdates) interaction);
            } else if (interaction.getTypeId().equals(VehicleRegistration.TYPE_ID)) {
                receiveInteraction((VehicleRegistration) interaction);
            } else if (interaction.getTypeId().equals(VehicleTypesInitialization.TYPE_ID)) {
                receiveInteraction((VehicleTypesInitialization) interaction);
            } else if (interaction.getTypeId().equals(ScenarioTrafficLightRegistration.TYPE_ID)) {
                receiveInteraction((ScenarioTrafficLightRegistration) interaction);
            } else if (interaction.getTypeId().equals(TrafficLightUpdates.TYPE_ID)) {
                receiveInteraction((TrafficLightUpdates) interaction);
            } else {
                log.info("Unused interaction: " + interaction.getTypeId() + " from " + interaction.getSenderId());
            }
        }
    }

    /**
     * Extract data from received {@link VehicleUpdates} interaction and apply
     * movements of externally simulated vehicles to CARLA via grpc calls.
     *
     * @param vehicleUpdates interaction indicating vehicles movements of a simulator
     */
    private synchronized void receiveInteraction(VehicleUpdates vehicleUpdates) throws InternalFederateException {
        if (vehicleUpdates == null || vehicleUpdates.getSenderId().equals(getId())) {
            log.debug("VehicleUpdates: null");
            return;
        }

        for (VehicleData updatedVehicle : vehicleUpdates.getUpdated()) {
            log.debug("VehicleUpdates: update " + updatedVehicle.getName());
            if (this.registeredVehicles.containsKey(updatedVehicle.getName())) {
                client.updateVehicle(updatedVehicle, this.registeredVehicles.get(updatedVehicle.getName()), false);
            } else {
                log.info("Update for unregistered vehicle + " + updatedVehicle.getName() + " received. Ignoring.");
            }
        }

        for (String removed : vehicleUpdates.getRemovedNames()) {
            log.debug("VehicleUpdates: remove " + removed);
            client.removeVehicle(removed);
        }

        for (VehicleData addedVehicle : vehicleUpdates.getAdded()) {
            log.debug("VehicleUpdates: added " + addedVehicle.getName());
            if (this.registeredVehicles.containsKey(addedVehicle.getName())) {
                client.updateVehicle(addedVehicle, this.registeredVehicles.get(addedVehicle.getName()), true);
            } else {
                log.info("Update for unregistered vehicle + " + addedVehicle.getName() + " received. Ignoring.");
            }
        }
    }

    /**
     * Keeps track of spawned vehicles which are not controlled by Carla.
     * @param vehicleRegistration Interaction
     */
    private synchronized void receiveInteraction(VehicleRegistration vehicleRegistration) {
        if (!vehicleRegistration.getSenderId().equals(getId())) {
            this.registeredVehicles.put(vehicleRegistration.getMapping().getName(),
                    vehicleRegistration.getMapping().getVehicleType());
        }
    }

    /**
     * Keeps track of vehicle types.
     * @param vehicleTypesInitialization interaction
     */
    private synchronized void receiveInteraction(VehicleTypesInitialization vehicleTypesInitialization) {
        if (!vehicleTypesInitialization.getSenderId().equals(getId())) {
            for (String key : vehicleTypesInitialization.getTypes().keySet()) {
                registeredVehicleTypes.put(key, vehicleTypesInitialization.getTypes().get(key));
            }
        }
    }

    /**
     * Keeps track of registered traffic light group IDs.
     * @param scenarioTrafficLightRegistration interaction
     * @throws InternalFederateException
     */
    private synchronized void receiveInteraction(ScenarioTrafficLightRegistration scenarioTrafficLightRegistration)
            throws InternalFederateException{
        if (!scenarioTrafficLightRegistration.getSenderId().equals(getId())) {
            try {
                for (org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightGroup trafficLightGroup : scenarioTrafficLightRegistration.getTrafficLightGroups()) {
                    // remember registered group IDs
                    String groupId = trafficLightGroup.getGroupId();
                    log.info("New traffic light group ID registered: " + groupId + ". Number of signals: " + trafficLightGroup.getTrafficLights().size());
                    if (groupId.equals("394")) {

                        // TODO: read actual mapping from disk or database or ...
                        List<String> newTrafficLightGroup = new ArrayList<>();
                        newTrafficLightGroup.add("1621");
                        newTrafficLightGroup.add("1618");
                        newTrafficLightGroup.add("1619");
                        newTrafficLightGroup.add("1620");
                        registeredTrafficLightGroupIds.put(groupId, newTrafficLightGroup);
                        // generate traffic light info
                        carlaTrafficLightsInfo.put("1621", new TrafficLightInfo(0, 3));
                        carlaTrafficLightsInfo.put("1618", new TrafficLightInfo(1, 3));
                        carlaTrafficLightsInfo.put("1619", new TrafficLightInfo(2, 3));
                        carlaTrafficLightsInfo.put("1620", new TrafficLightInfo(3, 3));

                        // subscribe to traffic light group
                        TrafficLightSubscription trafficLightSubscription = new TrafficLightSubscription(nextTimeStep, groupId);
                        rti.triggerInteraction(trafficLightSubscription);
                    }
                }
            } catch (IllegalValueException e) {
                throw new InternalFederateException(e);
            }
            log.info("Number of registered traffic light group IDs: " + registeredTrafficLightGroupIds.size());
        }
    }

    private synchronized void receiveInteraction(TrafficLightUpdates trafficLightUpdates) {
        if (!trafficLightUpdates.getSenderId().equals(getId())) {
            for (String key : trafficLightUpdates.getUpdated().keySet()) {
                TrafficLightGroupInfo trafficLightGroupInfo = trafficLightUpdates.getUpdated().get(key);
//                System.out.println(trafficLightGroupInfo.getCurrentState());
            }
        }
    }

    /**
     * Process the TimeAdvanceGrant event and sends the corresponding grpc-call to the grpc server
     * to invoke the tick command and receive carla information.
     *
     * @param time The timestamp towards which the federate can advance it local time.
     * @throws InternalFederateException
     */
    @Override
    public synchronized void processTimeAdvanceGrant(long time) throws InternalFederateException {
        log.debug("Received process Time Advance Grant");
        if (client == null) {
            throw new InternalFederateException("Error during advance time (" + time + "): GRPC Client not yet ready.");
        }

        // send cached interactions
        for (Interaction interaction : interactionList) {
            processInteractionAdvanced(interaction, time);
        }
        interactionList.clear();

        if (time < nextTimeStep) {
            // process time advance only if time is equal or greater than the next simulation time step
            return;
        }

        // interact with Carla
        try {
            // send simulationStep to tick carla simulation
            StepResult carlaStepResult = client.simulationStep();
            // TODO: get step size (100) from config file
            this.nextTimeStep += 100 * TIME.MILLI_SECOND;

            // add Carla controlled vehicles
            if (carlaStepResult.getAddActorsList().size() > 0) {
                spawnCarlaVehicles(carlaStepResult.getAddActorsList(), time);
            }

            // update Carla controlled vehicles (includes removed vehicles)
            if (carlaStepResult.getMoveActorsList().size() > 0) {
                updateCarlaVehicles(carlaStepResult.getMoveActorsList(), carlaStepResult.getRemoveActorsList(), time);
            }

            // update Carla controller traffic lights
            if (carlaStepResult.getTrafficLightUpdatesList().size() > 0) {
                setCarlaTrafficLightState(carlaStepResult.getTrafficLightUpdatesList());
            }

            rti.requestAdvanceTime(this.nextTimeStep, 0, (byte) 2);

        } catch (InternalFederateException | IllegalValueException e) {
            log.error("Error during advanceTime(" + time + ")", e);
            throw new InternalFederateException(e);
        }
    }

    /**
     * Sets the rti traffic light states according to the states in Carla.
     * @param trafficLights
     * @throws InternalFederateException
     */
    private void setCarlaTrafficLightState(List<TrafficLight> trafficLights) throws InternalFederateException {
        try {
            // process all traffic light changes
            for (TrafficLight trafficLight : trafficLights) {
                String carlaLandmarkId = trafficLight.getLandmarkId();
                if (carlaTrafficLightsInfo.containsKey(carlaLandmarkId)) {
                    switch (trafficLight.getState()) {
                        case "r":
                            carlaTrafficLightsInfo.get(carlaLandmarkId).setTrafficLightStates(TL_RED);
                            break;
                        case "G":
                            carlaTrafficLightsInfo.get(carlaLandmarkId).setTrafficLightStates(TL_GREEN);
                            break;
                        case "y":
                            carlaTrafficLightsInfo.get(carlaLandmarkId).setTrafficLightStates(TL_YELLOW);
                            break;
                        default:
                            throw new InternalFederateException();
                    }
                }
            }
            // for all known rti traffic light group IDs ...
            for (String rtiTrafficLightGroupId : registeredTrafficLightGroupIds.keySet()) {
                List<TrafficLightState> trafficLightStates = new ArrayList<>();
                // ... read traffic light state for each traffic light of the group
                List<String> carlaLandmarkIdsOfGroup = registeredTrafficLightGroupIds.get(rtiTrafficLightGroupId);
                for (String carlaLandmarkId : carlaLandmarkIdsOfGroup) {
                    trafficLightStates.addAll(carlaTrafficLightsInfo.get(carlaLandmarkId).getTrafficLightStates());
                }
                // pack new traffic light state information and send to rti
                TrafficLightStateChange trafficLightStateChange = new TrafficLightStateChange(nextTimeStep, rtiTrafficLightGroupId);
                trafficLightStateChange.setCustomState(trafficLightStates);
                rti.triggerInteraction(trafficLightStateChange);
            }
        } catch (IllegalValueException e) {
            throw new InternalFederateException(e);
        }
    }

    /**
     * Adds new vehicles to the simulation that are controlled by Carla.
     * @param spawnRequests List of spawn requests for Carla controlled vehicles
     * @param time Time of next simulation step
     * @throws InternalFederateException
     */
    private void spawnCarlaVehicles(List<SpawnRequest> spawnRequests, long time) throws InternalFederateException {
        try {
            log.info("Adding " + spawnRequests.size() + " Carla controlled vehicle(s) to the simulation");

            for (SpawnRequest spawnRequest : spawnRequests) {
                String vehicleId = spawnRequest.getActorId();

                VehicleType vehicleType;
                if (registeredVehicleTypes.containsKey(spawnRequest.getTypeId())) {
                    vehicleType = registeredVehicleTypes.get(spawnRequest.getTypeId());
                } else {
                    VehicleClass vehicleClass = client.getMosaicVehicleClassFromSumoVehicleClass(spawnRequest.getClassId());
                    vehicleType = new VehicleType(spawnRequest.getTypeId(), spawnRequest.getLength(),
                            spawnRequest.getWidth(), spawnRequest.getHeight(), null, null,
                            vehicleClass, null, null, null, null, null,
                            null, spawnRequest.getColor(), null, null);
                }

                // define vehicle route TODO: better definition of route
                String routeId = spawnRequest.getRoute();
                List<String> edgeList = new ArrayList<>();
                edgeList.add("-46.0.00");
                VehicleRoute vehicleRoute = new VehicleRoute(routeId, edgeList, null, 0.1);
                VehicleRouteRegistration vehicleRouteRegistration = new VehicleRouteRegistration(time, vehicleRoute);
                try {
                    rti.triggerInteraction(vehicleRouteRegistration);
                } catch (Exception e) {
                    log.error("Could not create route for Carla vehicle");
                }

                // register vehicle to rti
                // TODO: better selection of departure lane and speed
                VehicleDeparture vehicleDeparture = new VehicleDeparture.Builder(routeId)
                        .departureLane(VehicleDeparture.LaneSelectionMode.FIRST, 1, 0)
                        .departureSpeed(0.0)
                        .create();
                VehicleRegistration vehicleRegistration = new VehicleRegistration(time, vehicleId, vehicleGroup,
                        Lists.newArrayList(), vehicleDeparture, vehicleType);
                rti.triggerInteraction(vehicleRegistration);

                // declare vehicle as externally controlled
                // TODO: define vehicle radius
                VehicleFederateAssignment vehicleFederateAssignment = new VehicleFederateAssignment(time, vehicleId, getId(),
                        10);
                rti.triggerInteraction(vehicleFederateAssignment);
            }

        } catch (InternalFederateException e) {
            log.error("Could not spawn Carla vehicle (InternalFederateException)");
        } catch (IllegalValueException e) {
            log.error("Could not spawn Carla vehicle (IllegalValueException)");
            throw new InternalFederateException(e);
        }
    }

    /**
     * Updates vehicles that are controlled by Carla.
     * @param moveRequests List of move requests for Carla controlled vehicles
     * @param destroyRequests List of destroy requests for Carla controlled vehicles
     * @param time Time of next simulation step
     * @throws InternalFederateException
     */
    private void updateCarlaVehicles(List<MoveRequest> moveRequests, List<DestroyRequest> destroyRequests,
                                     long time) throws InternalFederateException {
        try {
            // update vehicles
            List<VehicleData> vehicleUpdateList = new ArrayList<>();
            for (MoveRequest moveRequest : moveRequests) {
                String vehicleId = moveRequest.getActorId();

                // compute vehicle's next position
                CartesianPoint cartesianPoint = CartesianPoint.xyz(moveRequest.getLocX(), moveRequest.getLocY(),
                        moveRequest.getLocZ());
                GeoPoint geoPoint = cartesianPoint.toGeo();

                // generate vehicle data and send to rti
                // TODO: set movement()?
                VehicleData vehicleData = new VehicleData.Builder(time, vehicleId)
                        .stopped(false)
                        .movement(0.0, 0.0, 0.0)
                        .signals(getVehicleSignals(moveRequest.getSignals()))
                        .position(geoPoint, cartesianPoint)
                        .orientation(getVehicleSignals(moveRequest.getSignals()).isReverseDrive() ?
                                        DriveDirection.BACKWARD : DriveDirection.FORWARD,
                                moveRequest.getYaw(), moveRequest.getSlope())
                        .create();
                vehicleUpdateList.add(vehicleData);
            }

            // remove vehicles
            List<String> vehicleRemovedList = new ArrayList<>();
            for (DestroyRequest destroyRequest : destroyRequests) {
                vehicleRemovedList.add(destroyRequest.getActorId());
            }

            // send updated and destroyed vehicles to rti
            VehicleUpdates vehicleUpdates = new VehicleUpdates(time,
                    Lists.newArrayList(),
                    vehicleUpdateList,
                    vehicleRemovedList);
            rti.triggerInteraction(vehicleUpdates);

        } catch (InternalFederateException e) {
            log.error("Could not update Carla vehicle (InternalFederateException)");
        } catch (IllegalValueException e) {
            log.error("Could not update Carla vehicle (IllegalValueException)");
            throw new InternalFederateException(e);
        }
    }

    private VehicleSignals getVehicleSignals(int signals) {
        boolean blinkerLeft = false;
        boolean blinkerRight = false;
        boolean blinkerEmergency = false;
        boolean brakeLight = false;
        boolean reverseDrive = false;
        if (((signals >> VEH_SIGNAL_BLINKER_RIGHT) & 1) == 1) {
            blinkerRight = true;
        }
        if (((signals >> VEH_SIGNAL_BLINKER_LEFT) & 1) == 1) {
            blinkerLeft = true;
        }
        if (((signals >> VEH_SIGNAL_BLINKER_EMERGENCY) & 1) == 1) {
            blinkerEmergency = true;
        }
        if (((signals >> VEH_SIGNAL_BRAKELIGHT) & 1) == 1) {
            brakeLight = true;
        }
        if (((signals >> VEH_SIGNAL_BACKDRIVE) & 1) == 1) {
            reverseDrive = true;
        }
        return new VehicleSignals(blinkerLeft, blinkerRight, blinkerEmergency, brakeLight, reverseDrive);
    }

    @Override
    public void finishSimulation() throws InternalFederateException {
        log.info("Closing Carla Resources");
        if (channel != null) {
            try {
                ((ManagedChannel) channel).shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Could not properly stop grpc client");
            }
        }
        log.info("Finished simulation");
    }

    @Override
    public boolean isTimeConstrained() {
        return true;
    }

    @Override
    public boolean isTimeRegulating() {
        return true;
    }

}
