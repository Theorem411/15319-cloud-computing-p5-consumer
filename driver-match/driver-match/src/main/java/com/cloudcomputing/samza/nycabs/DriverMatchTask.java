package com.cloudcomputing.samza.nycabs;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

private class DriverInfo {
    private Boolean availability;
    private Float longitude;
    private Float latitude;
    private Float rating;
    private String gender;
    private Integer salary;

    DriverInfo() {
    }

    public Boolean isAvailable() {
        return availability != null && availability;
    }

    public void update(Float longitude, Float latitude, Boolean availability,
            Float rating, Integer salary, String gender) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.availability = availability;
        this.rating = rating;
        this.salary = salary;
        this.gender = gender;
    }

    private Float getGenderScore(String genderPreference) {
        if (gender == null) {
            return null;
        }
        if (genderPreference.equals("N")) {
            return 1.0;
        } else {
            if (gender.equals(genderPreference)) {
                return 1.0;
            } else {
                return 0.0;
            }
        }
    }

    private Float getDistanceScore(Float clientLongitude, Float clientLatitude) {
        Float diffLongitude = clientLongitude - this.longitude;
        Float diffLatitude = clientLatitude - this.latitude;
        return Math.exp(
                -Math.sqrt(diffLongitude * diffLongitude + diffLatitude * diffLatitude));
    }

    private Float getRatingScore() {
        if (rating == null)
            return null;
        return rating / 5.0;
    }

    private Float getSalaryScore() {
        if (salary == null)
            return null;
        return 1 - (salary / 100.0);
    }

    /**
     * Computes match scores with this driver
     * 
     * @req only call when driver is properly initialized
     * @return null if this driver is invalid
     */
    public Float getMatchScore(Float clientLongitude, Float clientLatitude,
            String genderPreference) {
        Float genderScore = getGenderScore(genderPreference);
        if (genderScore == null) {
            return null;
        }
        Float distScore = getDistanceScore(clientLongitude, clientLatitude);
        if (distScore == null) {
            return null;
        }
        Float ratingScore = getRatingScore();
        if (ratingScore == null) {
            return null;
        }
        Float salaryScore = getSalaryScore();
        if (salaryScore == null) {
            return null;
        }

        return 0.4 * distScore + 0.1 * genderScore + 0.3 * ratingScore + 0.2 * salaryScore;
    }
}

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {
    /*
     * Define per task state here. (kv stores etc)
     * READ Samza API part in Writeup to understand how to start
     */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<Integer, Map<Integer, Object>> driversInBlock;

    private DriverInfo addOrCreateDriverInfo(Integer blockId, Integer driverId) {
        if (driversInBlock.get(blockId) == null) {
            driversInBlock.put(blockId, new HashMap<>());
        }
        Map<Integer, Object> driverMap = driversInBlock.get(blockId);
        if (driverMap.get(driverId) == null) {
            driverMap.put(driverId, new DriverInfo());
        }
        DriverInfo driverInfo = driverMap.get(driverId);
        return driverInfo;
    }

    private void deleteDriverInfo(Integer blockId, Integer driverId) {
        if (driversInBlock.get(blockId) == null) {
            driversInBlock.put(blockId, new HashMap<>());
        }
        Map<Integer, Object> blockMap = driversInBlock.get(blockId);
        blockMap.remove(driverId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driversInBlock = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext()
                .getStore("drivers-in-block");
    }

    private Boolean statusToBool(String status) {
        if (status.equals("AVAILABLE")) {
            return true;
        } else if (status.equals("UNAVAILABLE")) {
            return false;
        } else {
            throw new IllegalArgumentException("Invalid status value: " + status);
        }
    }

    /**
     * delete driver info from the block
     */
    private void processLeavingBlock(Integer blockId, Integer driverId, Float longitude,
            Float latitude, String status) {
        deleteDriverInfo(blockId, driverId);
    }

    /**
     * add or update driver info
     */
    private void processEnteringBlock(Integer blockId, Integer driverId, Float longitude,
            Float latitude, String status, Float rating, Integer salary, String gender) {
        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId);
        driverInfo.update(longitude, latitude, statusToBool(status), rating, salary, gender);
    }

    /**
     * add or update driver info
     */
    private void processRideComplete(Integer blockId, Integer driverId, Float longitude,
            Float latitude, Float rating, Integer salary, String gender) {
        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId);
        String status = "AVAILABLE";
        driverInfo.update(longitude, latitude, statusToBool(status), rating, salary, gender);
    }

    /**
     * find available driver with the maximum matching score in the same block
     * feed to output stream
     */
    private void processRideQuest(Integer blockId, Integer clientId, Float longitude,
            Float latitude, String genderPreference, MessageCollector collector) {
        if (driversInBlock.get(blockId) == null) {
            driversInBlock.put(blockId, new HashMap<>());
        }
        Map<String, Object> driverMap = driversInBlock.get(blockId);
        // find driver with best matching score
        Integer bestMatchId = null;
        Float bestMatchScore = -1.0;
        for (Map.Entry<Integer, DriverInfo> entry : driverMap.entrySet()) {
            Integer driverId = entry.getKey();
            DriverInfo driverInfo = entry.getValue();
            if (driverInfo.isAvailable()) {
                Float matchScore = driverInfo.getMatchScore(
                        longitude, latitude, genderPreference);
                if (matchScore != null && matchScore > bestMatchScore) {
                    bestMatchId = driverId;
                    bestMatchScore = matchScore;
                }
            }
        }
        // emit clientId bestmatchId pair to output stream
        if (bestMatchId != null) {
            Map<String, Integer> messageMap;
            messageMap.put("clientId", clientId);
            messageMap.put("driverId", bestMatchId);
            OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
                    DriverMatchConfig.MATCH_STREAM.getStream(), messageMap);
            collector.send(envelope);
        }
    }

    /**
     * process event stream message
     */
    private void processEvents(Map<String, Object> msg, MessageCollector collector) {
        Integer blockId = (Integer) msg.get("blockId");
        Float longitude = (Float) msg.get("longitude");
        Float latitude = (Float) msg.get("latitude");

        String type = (String) msg.get("type");
        if (type.equals("LEAVING_BLOCK")) {
            Integer driverId = (Integer) msg.get("driverId");
            String status = (String) msg.get("available");
            processLeavingBlock(blockId, driverId, longitude, latitude, status);
        } else if (type.equals("ENTERING_BLOCK")) {
            Integer driverId = (Integer) msg.get("driverId");
            String status = (String) msg.get("available");
            Float rating = (Float) msg.get("rating");
            Integer salary = (Integer) msg.get("salary");
            String gender = (String) msg.get("gender");
            processEnteringBlock(blockId, driverId, longitude, latitude, status, rating, salary, gender);
        } else if (type.equals("RIDE_COMPLETE")) {
            Integer driverId = (Integer) msg.get("driverId");
            Float rating = (Float) msg.get("rating");
            Integer salary = (Integer) msg.get("salary");
            String gender = (String) msg.get("gender");
            processRideComplete(blockId, driverId, longitude, latitude, rating, salary, gender);
        } else if (type.equals("RIDE_REQUEST")) {
            Integer clientId = (Integer) msg.get("clientId");
            String genderPreference = (String) msg.get("gender_preference");
            processRideQuest(blockId, clientId, longitude, latitude, genderPreference, collector);
        } else {
            // wrong type for events stream
        }
    }

    /**
     * process driver-locations stream message
     */
    private void processDriverLocation(Map<String, Object> msg) {
        Integer blockId = (Integer) msg.get("blockId");
        Integer driverId = (Integer) msg.get("driverId");
        Float longitude = (Float) msg.get("longitude");
        Float latitude = (Float) msg.get("latitude");

        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId);
        driverInfo.update(longitude, latitude, null, null, null, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,
            TaskCoordinator coordinator) {
        /*
         * All the messsages are partitioned by blockId, which means the messages
         * sharing the same blockId will arrive at the same task, similar to the
         * approach that MapReduce sends all the key value pairs with the same key
         * into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Driver Location messages
            processEvents((Map<String, Object>) envelope.getMessage(), collector);
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            processDriverLocation((Map<String, Object>) envelope.getMessage());
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
