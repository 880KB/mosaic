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

package org.eclipse.mosaic.app.carla;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.interactions.vehicle.VehicleCarlaSensorActivation;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;

public class CarlaSensorApp extends AbstractApplication<VehicleOperatingSystem> {

    int DELAY = 200;

    @Override
    public void onStartup() {
        VehicleCarlaSensorActivation vehicleCarlaSensorActivation =
                new VehicleCarlaSensorActivation(getOs().getSimulationTime() + DELAY * TIME.MILLI_SECOND,
                        getOs().getId(), VehicleCarlaSensorActivation.SensorTypes.LiDAR, true);
        getOs().sendInteractionToRti(vehicleCarlaSensorActivation);
    }

    @Override
    public void onShutdown() {
        VehicleCarlaSensorActivation vehicleCarlaSensorActivation =
                new VehicleCarlaSensorActivation(getOs().getSimulationTime() + DELAY * TIME.MILLI_SECOND,
                        getOs().getId(), VehicleCarlaSensorActivation.SensorTypes.LiDAR, false);
        getOs().sendInteractionToRti(vehicleCarlaSensorActivation);
    }

    @Override
    public void processEvent(Event event) {

    }
}
