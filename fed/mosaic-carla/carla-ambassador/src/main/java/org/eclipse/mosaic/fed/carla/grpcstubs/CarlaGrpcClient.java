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

package org.eclipse.mosaic.fed.carla.grpcstubs;

import io.grpc.Channel;
import org.eclipse.mosaic.fed.carla.grpc.*;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleType;

/**
 * grpc-client that connects to the grpc-carla-server
 */
public class CarlaGrpcClient {
    private final CarlaLinkServiceGrpc.CarlaLinkServiceBlockingStub blockingStub;

    public CarlaGrpcClient(Channel channel) {
        blockingStub = CarlaLinkServiceGrpc.newBlockingStub(channel);
    }

    public StepResult simulationStep() {
        Step request = Step.newBuilder().build();
        return blockingStub.simulationStep(request);
    }

    public void addVehicle(VehicleData data, VehicleType vehicleType) {
        Vehicle request = Vehicle.newBuilder().setId(data.getName())
                .setTypeId(vehicleType.getName())
                // TODO: set class
                .setVclass("passenger")
                .setLength(String.valueOf(vehicleType.getLength()))
                // TODO: set width and height
                .setHeight("1.62")
                .setWidth("1.86")
                .setLocation(
                        Location.newBuilder().setX(data.getProjectedPosition().getX())
                                .setY(data.getProjectedPosition().getY())
                                .setZ(data.getProjectedPosition().getZ()))
                .setRotation(
                        Rotation.newBuilder().setSlope(data.getSlope())
                                .setAngle(data.getHeading()).build())
                // TODO: set signals
                .build();
        blockingStub.addVehicle(request);
    }

    public void updateVehicle(VehicleData data, VehicleType vehicleType) {
        Vehicle request = Vehicle.newBuilder().setId(data.getName())
                .setTypeId(vehicleType.getName())
                // TODO: set class
                .setVclass("passenger")
                .setLength(String.valueOf(vehicleType.getLength()))
                // TODO: set width and height
                .setHeight("1.62")
                .setWidth("1.86")
                .setLocation(
                        Location.newBuilder().setX(data.getProjectedPosition().getX())
                                .setY(data.getProjectedPosition().getY())
                                .setZ(data.getProjectedPosition().getZ()))
                .setRotation(
                        Rotation.newBuilder().setSlope(data.getSlope())
                                .setAngle(data.getHeading()).build())
                // TODO: set signals
                .build();
        blockingStub.updateVehicle(request);
    }

    public void removeVehicle(String vehicleId) {
        Vehicle request = Vehicle.newBuilder().setId(vehicleId).build();
        blockingStub.removeVehicle(request);
    }

}
