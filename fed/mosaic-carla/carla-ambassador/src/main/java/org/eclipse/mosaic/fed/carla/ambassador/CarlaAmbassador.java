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

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.eclipse.mosaic.fed.carla.grpcstubs.CarlaGrpcClient;
import org.eclipse.mosaic.interactions.traffic.VehicleUpdates;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.rti.api.AbstractFederateAmbassador;
import org.eclipse.mosaic.rti.api.IllegalValueException;
import org.eclipse.mosaic.rti.api.Interaction;
import org.eclipse.mosaic.rti.api.InternalFederateException;
import org.eclipse.mosaic.rti.api.parameters.AmbassadorParameter;

import java.util.ArrayList;
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

        if (interaction.getTypeId().equals(VehicleUpdates.TYPE_ID)) {
            receiveInteraction((VehicleUpdates) interaction);
        } else {
//            log.warn(UNKNOWN_INTERACTION + interaction.getTypeId());
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
            client.updateVehicle(updatedVehicle);
        }

        for (String removed : vehicleUpdates.getRemovedNames()) {
            log.debug("VehicleUpdates: remove " + removed.toString());
            client.removeVehicle(removed);
        }

        for (VehicleData addedVehicle : vehicleUpdates.getAdded()) {
            log.debug("VehicleUpdates: added " + addedVehicle.getName());
            client.addVehicle(addedVehicle);
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
        // send simulationStep to tick carla simulation
        // TODO transfer carla data to mosaic
        client.simulationStep();

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
