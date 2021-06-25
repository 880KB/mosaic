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
import org.eclipse.mosaic.interactions.vehicle.VehicleCarlaSensorActivation;
import org.eclipse.mosaic.lib.enums.VehicleClass;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleData;
import org.eclipse.mosaic.lib.objects.vehicle.VehicleSignals;
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

    /**
     * defined vehicle signal bits by Sumo (https://sumo.dlr.de/docs/TraCI/Vehicle_Signalling.html)
     */
    private final int VEH_SIGNAL_BLINKER_RIGHT = 0;
    private final int VEH_SIGNAL_BLINKER_LEFT = 1;
    private final int VEH_SIGNAL_BLINKER_EMERGENCY = 2;
    private final int VEH_SIGNAL_BRAKELIGHT = 3;
    private final int VEH_SIGNAL_BACKDRIVE = 7;

    /**
     * Computes the vehicle signals represented as an int according to Sumo logic
     * (https://sumo.dlr.de/docs/TraCI/Vehicle_Signalling.html)
     * @param vehicleSignals Vehicle's signals
     * @return Vehicle's signals represented as int
     */
    private int getVehicleSignalsSumoStyle(VehicleSignals vehicleSignals) {
        int signals = 0;
        if (vehicleSignals.isBlinkerRight()) {
            signals += Math.pow(2, VEH_SIGNAL_BLINKER_RIGHT);
        }
        if (vehicleSignals.isBlinkerLeft()) {
            signals += Math.pow(2, VEH_SIGNAL_BLINKER_LEFT);
        }
        if (vehicleSignals.isBlinkerEmergency()) {
            signals += Math.pow(2, VEH_SIGNAL_BLINKER_EMERGENCY);
        }
        if (vehicleSignals.isBrakeLight()) {
            signals += Math.pow(2, VEH_SIGNAL_BRAKELIGHT);
        }
        if (vehicleSignals.isReverseDrive()) {
            signals += Math.pow(2, VEH_SIGNAL_BACKDRIVE);
        }
        return signals;
    }

    /**
     * Translates Mosaic vehicle classes to Sumo vehicle classes (which are used by Carla).
     * (https://sumo.dlr.de/docs/Definition_of_Vehicles%2C_Vehicle_Types%2C_and_Routes.html#abstract_vehicle_class)
     * @param vClass Mosaic vehicle class
     * @return Sumo vehicle class
     */
    private String getSumoVehicleClassFromMosaicVehicleClass(VehicleClass vClass) {
        switch (vClass) {
            case Unknown:
            case Car:
            case AutomatedVehicle:
                return "passenger";
            case LightGoodsVehicle:
            case WorksVehicle:
                return "delivery";
            case HeavyGoodsVehicle:
                return "truck";
            case PublicTransportVehicle:
                return "bus";
            case EmergencyVehicle:
                return "emergency";
            case VehicleWithTrailer:
                return "trailer";
            case MiniBus:
                return "coach";
            case Taxi:
                return "taxi";
            case ElectricVehicle:
                return "evehicle";
            case Bicycle:
                return "bicycle";
            case Motorcycle:
                return "motorcycle";
            case HighOccupancyVehicle:
                return "hov";
            case ExceptionalSizeVehicle:
            case HighSideVehicle:
            default:
                return "ignoring";
        }
    }

    /**
     * Translates Sumo vehicle classes (which are used by Carla) to Mosaic vehicle classes.
     * (https://sumo.dlr.de/docs/Definition_of_Vehicles%2C_Vehicle_Types%2C_and_Routes.html#abstract_vehicle_class)
     * @param vClass Sumo vehicle class
     * @return Mosaic vehicle class
     */
    public VehicleClass getMosaicVehicleClassFromSumoVehicleClass(String vClass) {
        switch (vClass) {
            case "passenger":
                return VehicleClass.Car;
            case "delivery":
                return VehicleClass.LightGoodsVehicle;
            case "truck":
                return VehicleClass.HeavyGoodsVehicle;
            case "bus":
                return VehicleClass.PublicTransportVehicle;
            case"emergency":
                return VehicleClass.EmergencyVehicle;
            case "trailer":
                return VehicleClass.VehicleWithTrailer;
            case "coach":
                return VehicleClass.MiniBus;
            case "taxi":
                return VehicleClass.Taxi;
            case "evehicle":
                return VehicleClass.ElectricVehicle;
            case "bicycle":
                return VehicleClass.Bicycle;
            case "motorcycle":
                return VehicleClass.Motorcycle;
            case "hov":
                return VehicleClass.HighOccupancyVehicle;
            case "ignoring":
            default:
                return VehicleClass.Unknown;
        }
    }

    /**
     * Sends car data to Carla.
     * @param data Vehicle data
     * @param vehicleType Vehicle type
     * @param isNewVehicle True if vehicle was newly spawned
     */
    public void updateVehicle(VehicleData data, VehicleType vehicleType, boolean isNewVehicle) {
        Vehicle request = Vehicle.newBuilder().setId(data.getName())
                .setTypeId(vehicleType.getName())
                .setVclass(getSumoVehicleClassFromMosaicVehicleClass(vehicleType.getVehicleClass()))
                .setLength(String.valueOf(vehicleType.getLength()))
                .setWidth(String.valueOf(vehicleType.getWidth()))
                .setHeight(String.valueOf(vehicleType.getHeight()))
                .setLocation(
                        Location.newBuilder().setX(data.getProjectedPosition().getX())
                                .setY(data.getProjectedPosition().getY())
                                .setZ(data.getProjectedPosition().getZ()))
                .setRotation(
                        Rotation.newBuilder().setSlope(data.getSlope())
                                .setAngle(data.getHeading()).build())
                .setSignals(getVehicleSignalsSumoStyle(data.getVehicleSignals()))
                .setColor(vehicleType.getColor() != null ? vehicleType.getColor() : "255,255,255,100")
                .build();
        if (isNewVehicle) {
            blockingStub.addVehicle(request);
        } else {
            blockingStub.updateVehicle(request);
        }
    }

    public void removeVehicle(String vehicleId) {
        Vehicle request = Vehicle.newBuilder().setId(vehicleId).build();
        blockingStub.removeVehicle(request);
    }

    public void updateTrafficLight(String landmarkId, String state) {
        TrafficLight trafficLight = TrafficLight.newBuilder()
                .setLandmarkId(landmarkId)
                .setState(state)
                .build();
        blockingStub.updateTrafficLight(trafficLight);
    }

    public String spawnSensor(String vehicleId, VehicleCarlaSensorActivation.SensorTypes sensorType) {
        Sensor sensor = Sensor.newBuilder()
                .setId(vehicleId)
                .setTypeId(sensorType.toString())
                .build();
        Sensor response = blockingStub.addSensor(sensor);
        // return Carla ID of spawned sensor
        String carlaSensorId = null;
        if (response.getAttributesList().size() > 0) {
            for (Attribute attribute : response.getAttributesList()) {
                if (attribute.getName().equals("sensor_id")) {
                    carlaSensorId = attribute.getValue();
                }
            }
        }
         return carlaSensorId;
    }
}
