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

    private static final long serialVersionUID = -7517846771157374259L;

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
     * Path to Carla.
     */
    public String pathToCarla = "";

    /**
     * Filename of the Carla executable.
     */
    public String carlaExe = "";

    /**
     * Seconds to wait for Carla to start.
     */
    public int secondsToWaitForCarla = 10;

    /**
     * Name of the map to be loaded by Carla's config.py.
     */
    public String carlaMap = "";

    /**
     * Toggles Carla autostart functionality.
     */
    public boolean carlaAutostart = false;

    /**
     * Number of lasers.
     * For LiDAR parameters see https://carla.readthedocs.io/en/0.9.11/ref_sensors/#lidar-sensor
     */
    public int lidarChannels = 32;

    /**
     * Maximum distance to measure/raycast in meters.
     */
    public double lidarRange = 10.0;

    /**
     * Points generated by all lasers per second.
     */
    public int lidarPointsPerSecond = 56000;

    /**
     * LIDAR rotation frequency.
     */
    public double lidarRotationFrequency = 10.0;

    /**
     * Angle in degrees of the highest laser.
     */
    public double lidarUpperFov = 10.0;

    /**
     * Angle in degrees of the lowest laser.
     */
    public double lidarLowerFov = -30.0;

    /**
     * Coefficient that measures the LIDAR intensity loss per meter.
     * See https://carla.readthedocs.io/en/0.9.11/ref_sensors/#lidar-sensor
     */
    public double lidarAtmosphereAttenuationRate = 0.004;

    /**
     * General proportion of points that are randomly dropped.
     */
    public double lidarDropoffGeneralRate = 0.45;

    /**
     * For the intensity based drop-off, the threshold intensity value above which no points are dropped.
     */
    public double lidarDropoffIntensityLimit = 0.8;

    /**
     * For the intensity based drop-off, the probability of each point with zero intensity being dropped.
     */
    public double lidarDropoffZeroIntensity = 0.4;

    /**
     * Standard deviation of the noise model to disturb each point along the vector of its raycast.
     */
    public double lidarNoiseStdDev = 0.0;

    public boolean carlaIsTlsManager() {
        return tlsManager.equals("carla");
    }

    public boolean mosaicIsTlsManager() {
        return tlsManager.equals("mosaic");
    }
}
