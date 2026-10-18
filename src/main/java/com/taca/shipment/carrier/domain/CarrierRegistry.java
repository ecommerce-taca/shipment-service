package com.taca.shipment.carrier.domain;

import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.domain.Carrier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class CarrierRegistry {
    private final Map<Carrier, CarrierAdapter> adapters = new EnumMap<>(Carrier.class);

    public CarrierRegistry(List<CarrierAdapter> adapters) {
        for (CarrierAdapter adapter : adapters)
            this.adapters.put(adapter.carrier(), adapter);
    }

    public CarrierAdapter require(Carrier carrier) {
        CarrierAdapter adapter = adapters.get(carrier);
        if (adapter == null)
            throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE", "Carrier adapter is not configured",
                    HttpStatus.SERVICE_UNAVAILABLE);
        return adapter;
    }

    public boolean configured(Carrier carrier) {
        return adapters.containsKey(carrier);
    }
}
