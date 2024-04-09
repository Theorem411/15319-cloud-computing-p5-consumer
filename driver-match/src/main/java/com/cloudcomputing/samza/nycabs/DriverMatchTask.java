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

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {
    private class DriverInfo {
        private String driverId;
        private Boolean availability;
        private Double longitude;
        private Double latitude;
        private Double rating;
        private String gender;
        private Integer salary;
    
        DriverInfo(String driverId, Double longitude, Double latitude) {
            this.driverId = driverId;
            this.longitude = longitude;
            this.latitude = latitude;
            this.availability = false;
        }

        public String toString() {
            return "{\ndriverId: " + String.valueOf(driverId) + ", " 
                + "avail: " + String.valueOf(availability) + ", "
                + "longitude: " + String.valueOf(longitude) + ", "
                + "latitude: " + String.valueOf(latitude) + ", "
                + "rating: " + String.valueOf(rating) + ", "
                + "gender: " + String.valueOf(gender) + ", "
                + "salary: " + String.valueOf(salary)
                + "}\n";
        }
    
        public Boolean isAvailable() {
            return availability != null && availability;
        }
    
        public void updateAvailable(Boolean availability) {
            this.availability = availability;
        }

        public void updateRating(Double rating) {
            this.rating = rating;
        }

        public void updateSalary(Integer salary) {
            this.salary = salary;
        }

        public void updateGender(String gender) {
            this.gender = gender;
        }

        public void updatePosition(Double longitude, Double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    
        private Double getGenderScore(String genderPreference) {
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
    
        private Double getDistanceScore(Double clientLongitude, Double clientLatitude) {
            Double diffLongitude = clientLongitude - this.longitude;
            Double diffLatitude = clientLatitude - this.latitude;
            return Math.exp(
                    -Math.sqrt(diffLongitude * diffLongitude + diffLatitude * diffLatitude));
        }
    
        private Double getRatingScore() {
            return this.rating / 5.0;
        }
    
        private Double getSalaryScore() {
            return 1 - (this.salary / MAX_MONEY);
        }
    
        /**
         * Computes match scores with this driver
         * 
         * @req only call when driver is properly initialized
         * @return null if this driver is invalid
         */
        public Double getMatchScore(Double clientLongitude, Double clientLatitude,
                String genderPreference) {
            Double genderScore = getGenderScore(genderPreference);
            Double distScore = getDistanceScore(clientLongitude, clientLatitude);
            Double ratingScore = getRatingScore();
            Double salaryScore = getSalaryScore();
            return 0.4 * distScore + 0.1 * genderScore + 0.3 * ratingScore + 0.2 * salaryScore;
        }
    }
    
    /*
     * Define per task state here. (kv stores etc)
     * READ Samza API part in Writeup to understand how to start
     */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, Map<String, Object>> driversLoc;

    private DriverInfo addOrCreateDriverInfo(String blockId, String driverId, Double longitude, Double latitude) {
        if (driversLoc.get(blockId) == null) {
            driversLoc.put(blockId, new HashMap<>());
        }
        Map<String, Object> driverMap = driversLoc.get(blockId);
        if (driverMap.get(driverId) == null) {
            driverMap.put(driverId, new DriverInfo(driverId, longitude, latitude));
        }
        DriverInfo driverInfo = (DriverInfo) driverMap.get(driverId);
        return driverInfo;
    }

    private void deleteDriverInfo(String blockId, String driverId) {
        if (driversLoc.get(blockId) == null) {
            driversLoc.put(blockId, new HashMap<>());
        }
        Map<String, Object> blockMap = driversLoc.get(blockId);
        blockMap.remove(driverId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driversLoc = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext()
                .getStore("driver-loc");
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
    private void processLeavingBlock(String blockId, String driverId, Double longitude,
            Double latitude, String status) {
        deleteDriverInfo(blockId, driverId);
    }

    /**
     * add or update driver info
     */
    private void processEnteringBlock(String blockId, String driverId, Double longitude,
            Double latitude, String status, Double rating, Integer salary, String gender) {
        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId, longitude, latitude);
        driverInfo.updatePosition(longitude, latitude);
        driverInfo.updateAvailable(statusToBool(status));
        driverInfo.updateSalary(salary);
        driverInfo.updateRating(rating);
        driverInfo.updateGender(gender);
    }

    /**
     * add or update driver info
     */
    private void processRideComplete(String blockId, String driverId, Double longitude,
            Double latitude, Double rating, Double userRating, Integer salary, String gender) {
        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId, longitude, latitude);
        driverInfo.updatePosition(longitude, latitude);
        driverInfo.updateAvailable(statusToBool("AVAILABLE"));
        driverInfo.updateRating((rating + userRating) / 2.0);
        driverInfo.updateSalary(salary);
        driverInfo.updateGender(gender);
    }

    /**
     * find available driver with the maximum matching score in the same block
     * feed to output stream
     */
    private void processRideRequest(String blockId, String clientId, Double longitude,
            Double latitude, String genderPreference, MessageCollector collector) {
        if (driversLoc.get(blockId) == null) {
            driversLoc.put(blockId, new HashMap<>());
        }
        Map<String, Object> driverMap = driversLoc.get(blockId);
        // find driver with best matching score
        String bestMatchId = null;
        Double bestMatchScore = -1.0;
        for (Map.Entry<String, Object> entry : driverMap.entrySet()) {
            String driverId = entry.getKey();
            DriverInfo driverInfo = (DriverInfo) entry.getValue();
            if (driverInfo.isAvailable()) {
                Double matchScore = driverInfo.getMatchScore(longitude, latitude, genderPreference);
                if (matchScore != null && matchScore > bestMatchScore) {
                    bestMatchId = driverId;
                    bestMatchScore = matchScore;
                }
            }
        }
        // emit clientId bestmatchId pair to output stream
        if (bestMatchId != null) {
            Map<String, String> messageMap = new HashMap<>();
            messageMap.put("clientId", clientId);
            messageMap.put("driverId", bestMatchId);
            OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
                    DriverMatchConfig.MATCH_STREAM, messageMap);
            collector.send(envelope);
            // 
            DriverInfo driverInfo = (DriverInfo) driverMap.get(bestMatchId);
            driverInfo.updateAvailable(statusToBool("UNAVAILABLE"));
        }
    }

    /**
     * process event stream message
     */
    private void processEvents(Map<String, Object> msg, MessageCollector collector) {
        String blockId = msg.get("blockId").toString();
        Double longitude = (Double) msg.get("longitude");
        Double latitude = (Double) msg.get("latitude");

        String type = (String) msg.get("type");
        if (type.equals("LEAVING_BLOCK")) {
            String driverId = msg.get("driverId").toString();
            String status = (String) msg.get("status");
            processLeavingBlock(blockId, driverId, longitude, latitude, status);
        } else if (type.equals("ENTERING_BLOCK")) {
            String driverId = msg.get("driverId").toString();
            String status = (String) msg.get("status");
            Double rating = (Double) msg.get("rating");
            Integer salary = (Integer) msg.get("salary");
            String gender = (String) msg.get("gender");
            processEnteringBlock(blockId, driverId, longitude, latitude, status, rating, salary, gender);
        } else if (type.equals("RIDE_COMPLETE")) {
            String driverId = msg.get("driverId").toString();
            Double rating = (Double) msg.get("rating");
            Double userRating = (Double) msg.get("user_rating");
            Integer salary = (Integer) msg.get("salary");
            String gender = (String) msg.get("gender");
            processRideComplete(blockId, driverId, longitude, latitude, rating, userRating, salary, gender);
        } else if (type.equals("RIDE_REQUEST")) {
            String clientId = msg.get("clientId").toString();
            String genderPreference = (String) msg.get("gender_preference");
            processRideRequest(blockId, clientId, longitude, latitude, genderPreference, collector);
        } else {
            // wrong type for events stream
            throw new IllegalArgumentException("Events stream receives wrong type");
        }
    }

    /**
     * process driver-locations stream message
     */
    private void processDriverLocation(Map<String, Object> msg) {
        String blockId = msg.get("blockId").toString();
        String driverId = msg.get("driverId").toString();
        Double longitude = (Double) msg.get("longitude");
        Double latitude = (Double) msg.get("latitude");

        DriverInfo driverInfo = addOrCreateDriverInfo(blockId, driverId, longitude, latitude);
        driverInfo.updatePosition(longitude, latitude);
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

        if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Driver Location messages
            processEvents((Map<String, Object>) envelope.getMessage(), collector);
        } else if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Event messages
            processDriverLocation((Map<String, Object>) envelope.getMessage());
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
