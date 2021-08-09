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

import com.dcaiti.phabmacs.api.sim.sensor.LidarFrame;
import com.google.common.collect.Lists;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.eclipse.mosaic.fed.carla.config.CCarla;
import org.eclipse.mosaic.fed.carla.grpc.*;
import org.eclipse.mosaic.fed.carla.grpcstubs.CarlaGrpcClient;
import org.eclipse.mosaic.interactions.mapping.VehicleRegistration;
import org.eclipse.mosaic.interactions.mapping.advanced.ScenarioTrafficLightRegistration;
import org.eclipse.mosaic.interactions.traffic.*;
import org.eclipse.mosaic.interactions.vehicle.VehicleCarlaSensorActivation;
import org.eclipse.mosaic.interactions.vehicle.VehicleFederateAssignment;
import org.eclipse.mosaic.interactions.vehicle.VehicleRouteRegistration;
import org.eclipse.mosaic.lib.enums.DriveDirection;
import org.eclipse.mosaic.lib.enums.VehicleClass;
import org.eclipse.mosaic.lib.geo.CartesianPoint;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.geo.MutableCartesianPoint;
import org.eclipse.mosaic.lib.math.Matrix3d;
import org.eclipse.mosaic.lib.math.Vector3d;
import org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightGroup;
import org.eclipse.mosaic.lib.objects.trafficlight.TrafficLightState;
import org.eclipse.mosaic.lib.objects.vehicle.*;
import org.eclipse.mosaic.lib.util.objects.ObjectInstantiation;
import org.eclipse.mosaic.rti.TIME;
import org.eclipse.mosaic.rti.api.AbstractFederateAmbassador;
import org.eclipse.mosaic.rti.api.IllegalValueException;
import org.eclipse.mosaic.rti.api.Interaction;
import org.eclipse.mosaic.rti.api.InternalFederateException;
import org.eclipse.mosaic.rti.api.parameters.AmbassadorParameter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CarlaAmbassador extends AbstractFederateAmbassador {
    /**
     * Configuration object.
     */
    CCarla carlaConfig;

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
    private final String VEHICLE_GROUP = "carla-controlled";

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
     * List of all registered traffic light group IDs to the rti and the according Carla landmark ids of the traffic
     * lights belonging to this group. (rti group ID -> Carla landmark IDs)
     * The list of Carla landmark ids has to be ordered according to their Sumo pole index.
     */
    private HashMap<String, List<String>> registeredTrafficLightGroupIds = new HashMap<>();

    /**
     * Holds information about a Carla traffic light. (Carla landmark ID -> traffic light info)
     */
    private HashMap<String, TrafficLightPole> carlaTrafficLightsInfo = new HashMap<>();

    /**
     * Carla traffic lights of one group are only matched to a Mosaic traffic light group ID if at least one is at most
     * this distance away.
     */
    private final double MAX_DISTANCE_TRAFFIC_LIGHT_MATCH = 15.0;

    /**
     * List of all spawned Carla sensors: key = Carla sensor ID, value = Mosaic vehicle ID.
     * Is used to match sent Carla sensor data to Mosaic vehicles.
     */
    private HashMap<String, String> registeredCarlaSensors = new HashMap<>();

    /**
     * Carla vehicles will spawn on this edge (and are moved to their actual position after the first position update).
     */
    private String carlaSpawnEdge = null;

    public CarlaAmbassador(AmbassadorParameter ambassadorParameter) {
        super(ambassadorParameter);

        try {
            carlaConfig = new ObjectInstantiation<>(CCarla.class, log)
                    .readFile(ambassadorParameter.configuration);
        } catch (InstantiationException e) {
            log.error("Configuration object could not be instantiated: ", e);
        }

        loadCarlaTrafficLightGroups();

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
        log.info("Trying to connect tp GRPC Server (Carla)");
        String target = "localhost:50051";
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        client = new CarlaGrpcClient(channel);
    }

    /**
     * Loads all Carla landmark IDs from disk (previously written by Carla Mosaic Co-Simulation).
     */
    private void loadCarlaTrafficLightGroups() {
        JSONParser parser = new JSONParser();
        String filename = carlaConfig.pathToCarlaCoSimulation + "\\data\\traffic_light_mapping.json";
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(filename));
            for (Object trafficLightGroupId : jsonObject.keySet()) {
                JSONArray carlaLandmarks = (JSONArray) jsonObject.get(trafficLightGroupId);
                List<String> groupMembers = new ArrayList<>();
                // for each pole
                for (Object carlaLandmarkIndex : carlaLandmarks) {
                    JSONObject trafficLightPole = (JSONObject) carlaLandmarkIndex;
                    for (Object trafficLightData : trafficLightPole.keySet()) {
                        JSONArray data = (JSONArray) trafficLightPole.get(trafficLightData);
                        // iterate through landmark data
                        String id = null;
                        double posX = 0;
                        double posY = 0;
                        for (Object d : data) {
                            JSONObject info = (JSONObject) d;
                            if (info.get("landmark_id") != null) {
                                id = (String) info.get("landmark_id");
                            }
                            if (info.get("pos_x") != null) {
                                posX = Double.parseDouble((String) info.get("pos_x"));
                            }
                            if (info.get("pos_y") != null) {
                                posY = -Double.parseDouble((String) info.get("pos_y"));
                            }
                        }
                        // write traffic light information
                        groupMembers.add(id);
                        CartesianPoint pos = new MutableCartesianPoint(posX, posY, 0);
                        carlaTrafficLightsInfo.put(id, new TrafficLightPole(id, pos, groupMembers,
                                carlaConfig.tlsStrictConversion));
                    }
                }
                // update group members
                for (String carlaLandmarkId : groupMembers) {
                    carlaTrafficLightsInfo.get(carlaLandmarkId).setGroupMembers(groupMembers);
                }
            }
        } catch (IOException e) {
            log.error("Could not open Carla landmark IDs file. Traffic light synchronization will not work.");
        } catch (ParseException e) {
            log.error("Could not parse Carla landmark IDs file. Traffic light synchronization will not work.");
        }
        if (log.isDebugEnabled()) {
            for (String s : carlaTrafficLightsInfo.keySet()) {
                log.debug(carlaTrafficLightsInfo.get(s).toString());
            }
        }
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
            } else if (interaction.getTypeId().equals(VehicleRoutesInitialization.TYPE_ID)) {
                receiveInteraction((VehicleRoutesInitialization) interaction);
            } else if (interaction.getTypeId().equals(ScenarioTrafficLightRegistration.TYPE_ID)) {
                receiveInteraction((ScenarioTrafficLightRegistration) interaction);
            } else if (interaction.getTypeId().equals(TrafficLightUpdates.TYPE_ID)) {
                receiveInteraction((TrafficLightUpdates) interaction);
            } else if (interaction.getTypeId().equals(VehicleCarlaSensorActivation.TYPE_ID)) {
                receiveInteraction((VehicleCarlaSensorActivation) interaction);
            } else {
                log.info("Unused interaction: " + interaction.getTypeId() + " from " + interaction.getSenderId());
            }
        }
    }

    /**
     * Forwards a sensor spawn request (which was issued by the CarlaSensorApp) to Carla.
     *
     * @param vehicleCarlaSensorActivation Interaction
     */
    private synchronized void receiveInteraction(VehicleCarlaSensorActivation vehicleCarlaSensorActivation) {
        if (vehicleCarlaSensorActivation.isActivate()) {
            // add sensor
            String vehicleId = vehicleCarlaSensorActivation.getVehicleId();
            HashMap<String, String> parameters = new HashMap<>();
            parameters.put("channels", String.valueOf(carlaConfig.lidarChannels));
            parameters.put("range", String.valueOf(carlaConfig.lidarRange));
            parameters.put("points_per_second", String.valueOf(carlaConfig.lidarPointsPerSecond));
            parameters.put("rotation_frequency", String.valueOf(carlaConfig.lidarRotationFrequency));
            parameters.put("upper_fov", String.valueOf(carlaConfig.lidarUpperFov));
            parameters.put("lower_fov", String.valueOf(carlaConfig.lidarLowerFov));
            parameters.put("atmosphere_attenuation_rate", String.valueOf(carlaConfig.lidarAtmosphereAttenuationRate));
            parameters.put("dropoff_general_rate", String.valueOf(carlaConfig.lidarDropoffGeneralRate));
            parameters.put("dropoff_intensity_limit", String.valueOf(carlaConfig.lidarDropoffIntensityLimit));
            parameters.put("dropoff_zero_intensity", String.valueOf(carlaConfig.lidarDropoffZeroIntensity));
            parameters.put("noise_stddev", String.valueOf(carlaConfig.lidarNoiseStdDev));
            String sensorId = client.spawnSensor(vehicleId, vehicleCarlaSensorActivation.getSensor(), parameters);
            if (sensorId != null) {
                log.info("{} sensor spawned for vehicle {}. Sensor ID: {}", vehicleCarlaSensorActivation.getSensor(),
                        vehicleId, sensorId);
                registeredCarlaSensors.put(sensorId, vehicleId);
            } else {
                log.warn("{} Sensor spawn request for vehicle {} failed: no sensor ID was returned. Sensor will not work.",
                        vehicleCarlaSensorActivation.getSensor(), vehicleId);
            }
        } else {
            // remove sensor
            for (String sensorId : registeredCarlaSensors.keySet()) {
                if (registeredCarlaSensors.get(sensorId).equals(vehicleCarlaSensorActivation.getVehicleId())) {
                    log.info("Removing sensor " + sensorId + " for vehicle " + vehicleCarlaSensorActivation.getVehicleId());
                    client.removeSensor(sensorId, vehicleCarlaSensorActivation.getSensor());
                }
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
     * Extracts a single edge as spawn edge for Carla vehicles.
     * @param vehicleRoutesInitialization interaction
     */
    private synchronized void receiveInteraction(VehicleRoutesInitialization vehicleRoutesInitialization)
            throws InternalFederateException {
        Set<String> routeIds = vehicleRoutesInitialization.getRoutes().keySet();
        for (String routeId : routeIds) {
            if (vehicleRoutesInitialization.getRoutes().get(routeId).getLastConnectionId() != null) {
                carlaSpawnEdge = vehicleRoutesInitialization.getRoutes().get(routeId).getLastConnectionId();
                break;
            }
        }
        if (carlaSpawnEdge == null) {
            throw new InternalFederateException("No valid edge given in VehicleRoutesInitialization.");
        }
    }

    /**
     * Matches Mosaic traffic light group IDs and Carla landmark IDs (that are used for traffic lights).
     * @param scenarioTrafficLightRegistration interaction
     * @throws InternalFederateException
     */
    private synchronized void receiveInteraction(ScenarioTrafficLightRegistration scenarioTrafficLightRegistration)
            throws InternalFederateException{
        if (!scenarioTrafficLightRegistration.getSenderId().equals(getId())) {
            int unmatchedMosaicGroupIds = 0;
            try {
                for (TrafficLightGroup trafficLightGroup : scenarioTrafficLightRegistration.getTrafficLightGroups()) {
                    String mosaicGroupId = trafficLightGroup.getGroupId();

                    // compute closest Carla landmark ID
                    String closestCarlaLandmarkId = getClosestCarlaLandmarkId(trafficLightGroup.getFirstPosition()
                            .toCartesian());
                    if (closestCarlaLandmarkId == null) {
                        log.debug("Could not match Mosaic traffic light group ID " + mosaicGroupId +
                                " with any known Carla landmark ID. Ignoring Mosaic group ID.");
                        unmatchedMosaicGroupIds += 1;
                        continue;
                    }
                    List<String> carlaGroupMembers = carlaTrafficLightsInfo.get(closestCarlaLandmarkId).getGroupMembers();

                    // compute number of poles in Mosaic and Carla groups
                    int mosaicPoles = getNumberOfTrafficLightPolesInMosaicGroup(trafficLightGroup);
                    int carlaPoles = carlaGroupMembers.size();
                    if (mosaicPoles != carlaPoles) {
                        log.debug("Number of traffic light poles in Mosaic (" + mosaicPoles + ") and Carla (" +
                                carlaPoles + ") does not match. Ignoring Mosaic group ID " + mosaicGroupId + ".");
                        unmatchedMosaicGroupIds += 1;
                        continue;
                    }

                    // check if Carla landmark IDs have not been matched to another Mosaic traffic light group yet
                    boolean assignable = true;
                    for (String carlaLandmarkId : carlaGroupMembers) {
                        if (carlaTrafficLightsInfo.get(carlaLandmarkId).isMatched()) {
                            assignable = false;
                            break;
                        }
                    }
                    if (!assignable) {
                        log.debug("Carla landmark IDs " + carlaGroupMembers +
                                " have already been matched to another Mosaic traffic light group ID." +
                                " Ignoring Mosaic group ID " + mosaicGroupId + ".");
                        unmatchedMosaicGroupIds += 1;
                        continue;
                    }

                    // matching successful
                    // sort members clockwise around Mosaic traffic light group location (according to Sumo logic:
                    // https://sumo.dlr.de/docs/Simulation/Traffic_Lights.html#default_link_indices)
                    carlaGroupMembers.sort(new ClockwiseComparator(trafficLightGroup.getFirstPosition().toCartesian()));
                    log.debug("Matching Mosaic traffic light ID " + mosaicGroupId + " with Carla Landmark IDs " +
                            carlaGroupMembers + ".");

                    // save matching
                    registeredTrafficLightGroupIds.put(mosaicGroupId, carlaGroupMembers);
                    for (String carlaLandmarkId : carlaGroupMembers) {
                        carlaTrafficLightsInfo.get(carlaLandmarkId).setGroupMembers(carlaGroupMembers);
                        carlaTrafficLightsInfo.get(carlaLandmarkId).setMatched(true);
                        // compute number of states of traffic light pole
                        int numberOfStates = getNumberOfStatesOfTrafficLightPole(carlaLandmarkId, trafficLightGroup);
                        carlaTrafficLightsInfo.get(carlaLandmarkId).setNumberOfStates(numberOfStates);
                    }

                    // subscribe to traffic light group
                    TrafficLightSubscription trafficLightSubscription = new TrafficLightSubscription(nextTimeStep,
                            mosaicGroupId);
                    rti.triggerInteraction(trafficLightSubscription);
                }
            } catch (IllegalValueException e) {
                throw new InternalFederateException(e);
            }

            // log output
            int unmatchedCarlaLandmarkIds = 0;
            for (String carlaLandmarkId : carlaTrafficLightsInfo.keySet()) {
                if (!carlaTrafficLightsInfo.get(carlaLandmarkId).isMatched()) {
                    unmatchedCarlaLandmarkIds += 1;
                }
            }
            if (unmatchedMosaicGroupIds > 0) {
                log.warn("Unmatched Mosaic traffic light group IDs: " + unmatchedMosaicGroupIds);
            }
            if (unmatchedCarlaLandmarkIds > 0) {
                log.warn("Unmatched Carla landmark IDs: " + unmatchedCarlaLandmarkIds);
            }
            if (unmatchedMosaicGroupIds == 0 && unmatchedCarlaLandmarkIds == 0) {
                log.info("All known traffic lights matched between Carla and Mosaic.");
            }
        }
    }

    /**
     * Extract data from received {@link TrafficLightUpdates} interaction and apply
     * traffic light states to CARLA via grpc calls.
     *
     * @param trafficLightUpdates interaction indicating traffic light states
     */
    private synchronized void receiveInteraction(TrafficLightUpdates trafficLightUpdates) {
        if (carlaConfig.mosaicIsTlsManager() && !trafficLightUpdates.getSenderId().equals(getId())) {
            // for each traffic light group
            for (String groupId : trafficLightUpdates.getUpdated().keySet()) {
                List<TrafficLightState> statesOfGroup = trafficLightUpdates.getUpdated().get(groupId).getCurrentState();
                int processedStates = 0;
                // for each pole in current traffic light group
                for (String carlaLandmarkId : registeredTrafficLightGroupIds.get(groupId)) {
                    TrafficLightPole pole = carlaTrafficLightsInfo.get(carlaLandmarkId);
                    int numberOfStates = pole.getNumberOfStates();
                    List<TrafficLightState> statesOfPole = statesOfGroup.subList(processedStates, processedStates +
                            numberOfStates);
                    pole.setState(statesOfPole);
                    // send state to Carla
                    client.updateTrafficLight(carlaLandmarkId, pole.getCarlaState());
                    processedStates += numberOfStates;
                }
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
            this.nextTimeStep += carlaConfig.updateInterval * TIME.MILLI_SECOND;

            // add Carla controlled vehicles
            if (carlaStepResult.getAddActorsList().size() > 0) {
                spawnCarlaControlledVehicles(carlaStepResult.getAddActorsList(), time);
            }

            // update Carla controlled vehicles (includes removed vehicles)
            if (carlaStepResult.getMoveActorsList().size() > 0) {
                updateCarlaControlledVehicles(carlaStepResult.getMoveActorsList(), carlaStepResult.getRemoveActorsList(), time);
            }

            // update Carla controlled traffic lights
            if (carlaConfig.carlaIsTlsManager()) {
                if (carlaStepResult.getTrafficLightUpdatesList().size() > 0) {
                    updateCarlaControlledTrafficLights(carlaStepResult.getTrafficLightUpdatesList());
                }
            }

            // receive Carla sensor data
            if (carlaStepResult.getSensorDataList().size() > 0) {
                forwardCarlaSensorData(carlaStepResult.getSensorDataList());
            }

            rti.requestAdvanceTime(this.nextTimeStep, 0, (byte) 2);

        } catch (InternalFederateException | IllegalValueException e) {
            log.error("Error during advanceTime(" + time + ")", e);
            throw new InternalFederateException(e);
        }
    }

    private void forwardCarlaSensorData(List<SensorData> sensorDataList) throws InternalFederateException {
        List<VehicleData> vehicleUpdateList = new ArrayList<>();
        for (SensorData sensorData : sensorDataList) {
            // build list of LiDAR points
            List<LidarFrame.LidarPoint> lidarPoints = new ArrayList<>();
            for (Location location : sensorData.getLidarPointsList()) {
                Vector3d vector3d = new Vector3d(location.getX(), location.getZ(), location.getY() * -1);
                LidarFrame.LidarPoint lidarPoint = new LidarFrame.LidarPoint(vector3d, true);
                lidarPoints.add(lidarPoint);
            }

            // build LiDAR frame
            Matrix3d rotationMatrix = new Matrix3d().loadIdentity();
            Vector3d reference = new Vector3d(sensorData.getLocation().getX(), sensorData.getLocation().getY(),
                    sensorData.getLocation().getZ());
            double timestamp = this.nextTimeStep;
            double minRange = sensorData.getMinRange();
            double maxRange = sensorData.getMaxRange();
            LidarFrame lidarFrame = new LidarFrame(rotationMatrix, reference, lidarPoints, timestamp, minRange, maxRange);

            // add LiDAR frame to updated vehicle list
            String vehicleId = registeredCarlaSensors.get(sensorData.getId());
            // vehicle ID is known
            if (vehicleId != null) {
                VehicleData vehicleData = new VehicleData.Builder(this.nextTimeStep, vehicleId)
                        .additional(lidarFrame)
                        .create();
                vehicleUpdateList.add(vehicleData);
            }
        }
        // send LiDAR frames via VehicleUpdates to rti
        try {
            VehicleUpdates vehicleUpdates = new VehicleUpdates(this.nextTimeStep,
                    Lists.newArrayList(),
                    vehicleUpdateList,
                    Lists.newArrayList());
            rti.triggerInteraction(vehicleUpdates);
        } catch (InternalFederateException e) {
            log.error("Could not send Carla sensor data to rti (InternalFederateException)");
        } catch (IllegalValueException e) {
            log.error("Could not send Carla sensor data to rti (IllegalValueException)");
            throw new InternalFederateException(e);
        }
    }

    /**
     * Computes the number of states the given Carla traffic light controls based on the given Mosaic traffic light group.
     * @param carlaLandmarkId Carla landmark ID
     * @param trafficLightGroup Mosaic traffic light group
     * @return
     */
    private int getNumberOfStatesOfTrafficLightPole(String carlaLandmarkId, TrafficLightGroup trafficLightGroup) {
        List<String> groupMembers = carlaTrafficLightsInfo.get(carlaLandmarkId).getGroupMembers();
        int indexOfCarlaLandmark = groupMembers.indexOf(carlaLandmarkId);
        String currentIncomingEdge = "";
        int countedIncomingEdges = 0;
        int numberOfStates = 0;
        for (org.eclipse.mosaic.lib.objects.trafficlight.TrafficLight trafficLight : trafficLightGroup.getTrafficLights()) {
            // new incoming edge (aka new traffic pole)?
            String incomingLane = trafficLight.getIncomingLane();
            String incomingEdge = incomingLane.substring(0, incomingLane.lastIndexOf('_'));
            if (!incomingEdge.equals(currentIncomingEdge)) {
                currentIncomingEdge = incomingEdge;
                countedIncomingEdges += 1;
            }
            // count if index of pole + 1 matches number of counted incoming lanes
            if (countedIncomingEdges == indexOfCarlaLandmark + 1) {
                numberOfStates += 1;
            }
        }
        return numberOfStates;
    }

    /**
     * Computes the number of traffic poles in a Mosaic traffic lights group aka the number of incoming edges (not
     * lanes)
     * @param trafficLightGroup Mosaic traffic light group
     * @return number of traffic light poles
     */
    private int getNumberOfTrafficLightPolesInMosaicGroup(TrafficLightGroup trafficLightGroup) {
        Set<String> incomingEdges = new HashSet<>();
        for (org.eclipse.mosaic.lib.objects.trafficlight.TrafficLight tl : trafficLightGroup.getTrafficLights()) {
            String incomingLane = tl.getIncomingLane();
            incomingEdges.add(incomingLane.substring(0, incomingLane.lastIndexOf('_')));
        }
        return incomingEdges.size();
    }

    /**
     * Computes the closest Carla traffic light to a given {@link CartesianPoint} location.
     * @param location Position to compute the closest Carla traffic light to
     * @return Carla landmark Id of closest traffic light
     */
    private String getClosestCarlaLandmarkId(CartesianPoint location) {
        String closestCarlaLandmarkId = null;
        double minDistance = Double.POSITIVE_INFINITY;
        for (String carlaLandmarkId : carlaTrafficLightsInfo.keySet()) {
            if (location.distanceTo(carlaTrafficLightsInfo.get(carlaLandmarkId).getLocation()) < minDistance) {
                minDistance = location.distanceTo(carlaTrafficLightsInfo.get(carlaLandmarkId).getLocation());
                closestCarlaLandmarkId = carlaLandmarkId;
            }
        }
        if (minDistance <= MAX_DISTANCE_TRAFFIC_LIGHT_MATCH) {
            return closestCarlaLandmarkId;
        } else {
            log.debug("Distance exceeded during matching for " + location + ": " + minDistance + " > " +
                    MAX_DISTANCE_TRAFFIC_LIGHT_MATCH);
            return null;
        }
    }

    /**
     * Comparator for clockwise sorting of Carla landmark IDs. If the sorting does not fit to a specific scenario, e.g.,
     * because of European traffic light systems, this would be the place to change it.
     */
    class ClockwiseComparator implements Comparator<String> {
        private final CartesianPoint r;
        public ClockwiseComparator(CartesianPoint referencePoint) {
            r = referencePoint;
        }
        @Override
        public int compare(String o1, String o2) {
            double o1X = carlaTrafficLightsInfo.get(o1).getLocation().getX() - r.getX();
            double o1Y = carlaTrafficLightsInfo.get(o1).getLocation().getY() - r.getY();
            double o2X = carlaTrafficLightsInfo.get(o2).getLocation().getX() - r.getX();
            double o2Y = carlaTrafficLightsInfo.get(o2).getLocation().getY() - r.getY();
            // source: https://stackoverflow.com/questions/6989100/sort-points-in-clockwise-order
            if (o1X >= 0 && o2X < 0) {
                return 1;
            }
            if (o1X < 0 && o2X >= 0) {
                return -1;
            }
            if (o1X == 0 && o2X == 0) {
                return 0;
            }
            double crossProdukt = o1X * o2Y - o2X * o1Y;
            if (crossProdukt < 0) {
                return -1;
            }
            if (crossProdukt > 0) {
                return 1;
            }
            return 0;
        }
    }

    /**
     * Sets the rti traffic light states according to the states in Carla.
     * @param trafficLights
     * @throws InternalFederateException
     */
    private void updateCarlaControlledTrafficLights(List<TrafficLight> trafficLights) throws InternalFederateException {
        try {
            // process all traffic light changes
            for (TrafficLight trafficLight : trafficLights) {
                String carlaLandmarkId = trafficLight.getLandmarkId();
                if (carlaTrafficLightsInfo.containsKey(carlaLandmarkId)) {
                    carlaTrafficLightsInfo.get(carlaLandmarkId).setState(trafficLight.getState());
                }
            }
            // for all known rti traffic light group IDs ...
            for (String rtiTrafficLightGroupId : registeredTrafficLightGroupIds.keySet()) {
                List<TrafficLightState> trafficLightStates = new ArrayList<>();
                // ... read traffic light state for each traffic light of the group
                List<String> carlaLandmarkIdsOfGroup = registeredTrafficLightGroupIds.get(rtiTrafficLightGroupId);
                for (String carlaLandmarkId : carlaLandmarkIdsOfGroup) {
                    trafficLightStates.addAll(carlaTrafficLightsInfo.get(carlaLandmarkId).getMosaicStates());
                }
                // pack new traffic light state information and send to rti
                TrafficLightStateChange trafficLightStateChange = new TrafficLightStateChange(nextTimeStep,
                        rtiTrafficLightGroupId);
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
    private void spawnCarlaControlledVehicles(List<SpawnRequest> spawnRequests, long time)
            throws InternalFederateException {
        try {
            log.debug("Adding " + spawnRequests.size() + " Carla controlled vehicle(s) to the simulation");

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

                // define vehicle route
                String routeId = spawnRequest.getRoute();
                List<String> edgeList = new ArrayList<>();
                edgeList.add(carlaSpawnEdge);
                VehicleRoute vehicleRoute = new VehicleRoute(routeId, edgeList, null, 0.1);
                VehicleRouteRegistration vehicleRouteRegistration = new VehicleRouteRegistration(time, vehicleRoute);
                try {
                    rti.triggerInteraction(vehicleRouteRegistration);
                } catch (Exception e) {
                    log.error("Could not create route for Carla vehicle");
                }

                // register vehicle to rti
                VehicleDeparture vehicleDeparture = new VehicleDeparture.Builder(routeId)
                        .departureLane(VehicleDeparture.LaneSelectionMode.BEST, 0, 0)
                        .departureSpeed(0.0)
                        .create();
                VehicleRegistration vehicleRegistration = new VehicleRegistration(time, vehicleId, VEHICLE_GROUP,
                        Lists.newArrayList(), vehicleDeparture, vehicleType);
                rti.triggerInteraction(vehicleRegistration);

                // declare vehicle as externally controlled
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
    private void updateCarlaControlledVehicles(List<MoveRequest> moveRequests, List<DestroyRequest> destroyRequests,
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
