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

package org.eclipse.mosaic.interactions.vehicle;

import org.eclipse.mosaic.rti.api.Interaction;

public final class VehicleCarlaSensorActivation extends Interaction {

    private static final long serialVersionUID = 1L;

    /**
     * String identifying the type of this interaction.
     */
    public final static String TYPE_ID = createTypeIdentifier(VehicleCarlaSensorActivation.class);

    /**
     * Possible sensor types to be spawned.
     */
    public enum SensorTypes {
        LiDAR
    }

    /**
     * Id of the vehicle the sensor is attached to.
     */
    private final String vehicleId;

    /**
     * Sensor type.
     */
    private final SensorTypes sensor;

    /**
     * Creates a new {@link VehicleCarlaSensorActivation} interaction.
     *
     * @param time Timestamp of this interaction, unit: [ms]
     */
    public VehicleCarlaSensorActivation(long time, String vehicleId, SensorTypes sensor) {
        super(time);
        this.vehicleId = vehicleId;
        this.sensor = sensor;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public SensorTypes getSensor() {
        return sensor;
    }

    // TODO: hashCode() equals() toString()
}
