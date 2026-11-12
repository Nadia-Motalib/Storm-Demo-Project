package com.machineanomalydetection;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(
            Map<String, Object> conf,
            TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int sensorId = input.getIntegerByField("sensor_id");
        int machineId = input.getIntegerByField("machine_id");
        double vibration = input.getDoubleByField("vibration");
        double temperature = input.getDoubleByField("temperature");

        boolean valid
                = sensorId > 0
                && machineId > 0
                && Double.isFinite(vibration)
                && Double.isFinite(temperature)
                && vibration >= 0.0 && vibration <= 12.0
                && temperature >= -20.0 && temperature <= 150.0;

        if (valid) {
            collector.emit(input, new Values(
                    sensorId,
                    machineId,
                    vibration,
                    temperature
            ));
        } else {
            System.out.println("[FILTER] Invalid reading discarded");
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "sensor_id",
                "machine_id",
                "vibration",
                "temperature"
        ));
    }
}
