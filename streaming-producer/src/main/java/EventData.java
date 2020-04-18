public class EventData {
    public static final String SOURCE_W = "W";
    public static final String SOURCE_T = "T";

    public static EventData CreateEventData(String data) throws Exception {
        String[] args = data.split(",");

        if (args.length == 19) {
            String newData = data + " ";
            args = newData.split(",");
        }

        if (args.length < 20) {
            return null;
        }

        return new EventData(args);
    }

    private final String eventId;
    private final String source;
    private final String type;
    private final String severity;
    private final String tmc;
    private final String description;
    private final String startTime;
    private final String endTime;
    private final String timeZone;
    private final String locationLat;
    private final String locationLng;
    private final String distance;
    private final String airportCode;
    private final String number;
    private final String street;
    private final String side;
    private final String city;
    private final String country;
    private final String state;
    private final String zipCode;

    private EventData(String[] args) {
        this.eventId = args[0];
        this.source = args[1];
        this.type = args[2];
        this.severity = args[3];
        this.tmc = args[4];
        this.description = args[5];
        this.startTime = args[6];
        this.endTime = args[7];
        this.timeZone = args[8];
        this.locationLat = args[9];
        this.locationLng = args[10];
        this.distance = args[11];
        this.airportCode = args[12];
        this.number = args[13];
        this.street = args[14];
        this.side = args[15];
        this.city = args[16];
        this.country = args[17];
        this.state = args[18];
        this.zipCode = args[19];
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", eventId, source,
                type, severity, tmc, description, startTime, endTime, timeZone, locationLat, locationLng, distance,
                airportCode, number, street, side, city, country, state, zipCode
        );
    }

    public String getEventId() {
        return eventId;
    }

    public String getSource() {
        return source;
    }

    public String getType() {
        return type;
    }

    public String getSeverity() {
        return severity;
    }

    public String getTmc() {
        return tmc;
    }

    public String getDescription() {
        return description;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getLocationLat() {
        return locationLat;
    }

    public String getLocationLng() {
        return locationLng;
    }

    public String getDistance() {
        return distance;
    }

    public String getAirportCode() {
        return airportCode;
    }

    public String getNumber() {
        return number;
    }

    public String getStreet() {
        return street;
    }

    public String getSide() {
        return side;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getState() {
        return state;
    }

    public String getZipCode() {
        return zipCode;
    }
}
