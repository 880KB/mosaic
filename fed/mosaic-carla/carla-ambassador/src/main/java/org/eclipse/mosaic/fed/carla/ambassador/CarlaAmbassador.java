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
import org.eclipse.mosaic.fed.carla.grpc.MoveRequest;
import org.eclipse.mosaic.fed.carla.grpc.SpawnRequest;
import org.eclipse.mosaic.fed.carla.grpc.StepResult;
import org.eclipse.mosaic.fed.carla.grpcstubs.CarlaGrpcClient;
import org.eclipse.mosaic.interactions.mapping.VehicleRegistration;
import org.eclipse.mosaic.interactions.traffic.VehicleUpdates;
import org.eclipse.mosaic.interactions.vehicle.VehicleFederateAssignment;
import org.eclipse.mosaic.interactions.vehicle.VehicleRouteRegistration;
import org.eclipse.mosaic.lib.enums.DriveDirection;
import org.eclipse.mosaic.lib.geo.CartesianPoint;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.vehicle.*;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.rti.api.AbstractFederateAmbassador;
import org.eclipse.mosaic.rti.api.IllegalValueException;
import org.eclipse.mosaic.rti.api.Interaction;
import org.eclipse.mosaic.rti.api.InternalFederateException;
import org.eclipse.mosaic.rti.api.parameters.AmbassadorParameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        // TODO: something is wrong
        CartesianPoint cartesianPoint = CartesianPoint.xyz(513.1, 423.80, 0.0);
        System.out.println("GEO: " + cartesianPoint.toGeo().toString());
        System.out.println(cartesianPoint + " : " + cartesianPoint.toGeo().toCartesian().toString());

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

        if (interaction.getTypeId().equals(VehicleUpdates.TYPE_ID)) {
            receiveInteraction((VehicleUpdates) interaction);
        } else if (interaction.getTypeId().equals(VehicleRegistration.TYPE_ID)) {
            receiveInteraction((VehicleRegistration) interaction);
        } else {
            log.info("Unused interaction: " + interaction.getTypeId() + " from " + interaction.getSenderId());
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
                client.updateVehicle(updatedVehicle, this.registeredVehicles.get(updatedVehicle.getName()));
            } else {
                log.info("Update for unregistered vehicle + " + updatedVehicle.getName() + " received. Ignoring.");
            }
        }

        for (String removed : vehicleUpdates.getRemovedNames()) {
            log.debug("VehicleUpdates: remove " + removed.toString());
            client.removeVehicle(removed);
        }

        for (VehicleData addedVehicle : vehicleUpdates.getAdded()) {
            log.debug("VehicleUpdates: added " + addedVehicle.getName());
            if (this.registeredVehicles.containsKey(addedVehicle.getName())) {
                client.addVehicle(addedVehicle, this.registeredVehicles.get(addedVehicle.getName()));
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

            // update Carla controlled vehicles
            if (carlaStepResult.getMoveActorsList().size() > 0) {
                updateCarlaVehicles(carlaStepResult.getMoveActorsList(), time);
            }

            rti.requestAdvanceTime(this.nextTimeStep, 0, (byte) 2);

        } catch (InternalFederateException | IllegalValueException e) {
            log.error("Error during advanceTime(" + time + ")", e);
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
            log.info("* adding " + spawnRequests.size() + " Carla controlled vehicle to the simulation: " +
                    spawnRequests.get(0).getActorId() + " with route id " + spawnRequests.get(0).getRoute());

            // TODO: loop over spawn requests
            SpawnRequest spawnRequest = spawnRequests.get(0);

            String vehicleId = spawnRequest.getActorId();
            // TODO: define vehicle type
            VehicleType vehicleType = new VehicleType(spawnRequest.getTypeId());
            // TODO: generate individual route id?
            String routeId = "carlaRoute";

            // Define vehicle route
            // TODO: better definition of route?
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
            // TODO: better selection of departure lane?
            // TODO: better selection of speed?
            VehicleDeparture vehicleDeparture = new VehicleDeparture.Builder(routeId)
                    .departureLane(VehicleDeparture.LaneSelectionMode.FIRST, 1, 0)
                    .departureSpeed(0.0)
                    .create();
            VehicleRegistration vehicleRegistration = new VehicleRegistration(time, vehicleId, vehicleGroup,
                    Lists.newArrayList(), vehicleDeparture, vehicleType);
            rti. triggerInteraction(vehicleRegistration);

            // declare vehicle as externally controlled
            // TODO: define vehicle radius
            VehicleFederateAssignment vehicleFederateAssignment = new VehicleFederateAssignment(time, vehicleId, getId(),
                    10);
            rti.triggerInteraction(vehicleFederateAssignment);

            // TODO: define vehicle color

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
     * @param time Time of next simulation step
     * @throws InternalFederateException
     */
    private void updateCarlaVehicles(List<MoveRequest> moveRequests, long time) throws InternalFederateException {
        try {
            // log.info("* Updating Carla vehicle " + moveRequests.get(0).getActorId());

            // TODO: loop over move requests
            MoveRequest moveRequest = moveRequests.get(0);

            String vehicleId = moveRequest.getActorId();

            // compute vehicle's next position
            // TODO: add z position?
            CartesianPoint cartesianPoint = CartesianPoint.xyz(moveRequest.getLocX(), moveRequest.getLocY(), 0.0);
            GeoPoint geoPoint = cartesianPoint.toGeo();

            // TODO: set vehicle's actual signals
            VehicleSignals vehicleSignals = new VehicleSignals(false, false,
                    false, false, false);

            // generate vehicle data and send to rti
            // TODO: what relevance have .stopped() and .movement()?
            // TODO: set actual drive direction
            // TODO: set actual slope
            VehicleData vehicleData = new VehicleData.Builder(time, vehicleId)
                    .stopped(false)
                    .movement(0.0, 0.0, 0.0)
                    .signals(vehicleSignals)
                    .position(geoPoint, cartesianPoint)
                    .orientation(DriveDirection.FORWARD, moveRequest.getYaw(), 0)
                    .create();
            // push vehicle data in list
            List<VehicleData> vehicleUpdateList = new ArrayList<>();
            vehicleUpdateList.add(vehicleData);
            VehicleUpdates vehicleUpdates = new VehicleUpdates(time,
                    Lists.newArrayList(),
                    vehicleUpdateList,
                    Lists.newArrayList());
            rti.triggerInteraction(vehicleUpdates);

        } catch (InternalFederateException e) {
            log.error("Could not update Carla vehicle (InternalFederateException)");
        } catch (IllegalValueException e) {
            log.error("Could not update Carla vehicle (IllegalValueException)");
            throw new InternalFederateException(e);
        }
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
