package com.machineanomalydetection;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class MqttSourceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private final Random random = new Random();

    @Override
    public void open(
            Map<String, Object> conf,
            TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        long timestamp = System.currentTimeMillis();

        int sensorId = random.nextInt(10) + 1;
        int machineId = random.nextInt(5) + 1;

        double vibration = random.nextDouble() * 8.0;

        // Generate occasional high-vibration readings.
        if (random.nextDouble() < 0.10) {
            vibration = 8.0 + random.nextDouble() * 4.0;
        }

        double temperature = 30.0 + random.nextDouble() * 70.0;

        collector.emit(new Values(
                timestamp,
                sensorId,
                machineId,
                vibration,
                temperature
        ));

        Utils.sleep(100);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "timestamp",
                "sensor_id",
                "machine_id",
                "vibration",
                "temperature"
        ));
    }
}
