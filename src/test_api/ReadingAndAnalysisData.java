package test_api;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ReadingAndAnalysisData {
	

    static Queue<JsonObject> q = new LinkedList<>();
    
    /* 6 */
    static int num_of_records = 0;
    /* 7 */
    static int num_of_trips = 0;
    
    /* 1 */
    static int[][] trips_per_day = new int[12][31];
    
    /* 3 */
    static int trips_without_dropOffLocation_yellow = 0;
    static int trips_without_dropOffLocation_green = 0;
    static int trips_without_dropOffLocation_fhv = 0;
    
    /* 5 */
    static int[][] trips_pickedup_Madison_Brooklyn_fhv = new int[12][31];
    static int[][] trips_pickedup_Madison_Brooklyn_yellow  = new int[12][31];
    static int[][] trips_pickedup_Madison_Brooklyn_green  = new int[12][31];
    
    /* 4 */
    static int num_of_trips_fhv = 0;
    static int num_of_trips_yellow = 0;
    static int num_of_trips_green = 0;
    static int num_of_minutes_fhv = 0;
    static int num_of_minutes_yellow = 0;
    static int num_of_minutes_green = 0;
    static int minutes_per_trip_fhv = 0;
    static int minutes_per_trip_yellow = 0;
    static int minutes_per_trip_green = 0;
    
    /* 8 */
    static int avg_trips_per_day = 0;
    static int num_of_days = 0;
    
    /* 10 */
    static int num_trips_pickedup_Woodside_Queens = 0;
    
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		try {
			
            // open websocket
            final WebsocketClientEndpoint clientEndPoint = new WebsocketClientEndpoint(new URI("ws://localhost:9000/ws"));

            // add listener
            clientEndPoint.addMessageHandler(new WebsocketClientEndpoint.MessageHandler() {
                public void handleMessage(String message) {
                    //addNewTrip(message);

                	Add_Trip(message);
                	num_of_records++;
                	
                }
            });
            TimeUnit.SECONDS.sleep(5);
           // TimeUnit.MILLISECONDS.sleep(500);
            print_message();
            
            

        } catch (URISyntaxException ex) {
            System.err.println("URISyntaxException exception: " + ex.getMessage());
        }
		

	}
	
	public static void Add_Trip(String message){
		JsonParser jsonParser = new JsonParser();
		JsonObject o = jsonParser.parse(message).getAsJsonObject();
		if(!q.contains(o)) {
    		q.add(o);
    		num_of_trips++;
    	}
	}
	
	public static void print_message() {
		
		while(!q.isEmpty()) {
			JsonObject o = q.poll();
			System.out.println(o);

			increase_trip_per_day(o);			/* 1 */
			increase_trip_without_dropOffLocation(o);		/* 3 */
			minutes_per_trip(o);			/* 4 */
			increase_trip_pickedup_madison(o);			/* 5 */
			increase_trip_pickedup_Woodside_Queens(o);		/* 10 */
			write_data_in_file();
		}
		
		avg_trips_per_day();		/* 8 */
		
		
		for(int i=0;i<trips_per_day.length;i++) {
			for(int j=0;j<trips_per_day[0].length;j++) {
				if(trips_per_day[i][j] != 0) {
					int month = i+1;
					int day = j+1;
					System.out.println("total_trips:" + "  " + "month: " + month + " " + "day: " + day + " " + "trips: " + trips_per_day[i][j]);
				}
				
				if(trips_pickedup_Madison_Brooklyn_fhv[i][j] != 0) {
					int month = i+1;
					int day = j+1;
					System.out.println("trips_pickedup_Madison_Brooklyn_fhv:" + "  " + "month: " + month + " " + "day: " + day + " " + "trips: " + trips_pickedup_Madison_Brooklyn_fhv[i][j]);
				}
				
				if(trips_pickedup_Madison_Brooklyn_yellow[i][j] != 0) {
					int month = i+1;
					int day = j+1;
					System.out.println("trips_pickedup_Madison_Brooklyn_yellow:" + "  " + "month: " + month + " " + "day: " + day + " " + "trips: " + trips_pickedup_Madison_Brooklyn_yellow[i][j]);
				}
				
				if(trips_pickedup_Madison_Brooklyn_green[i][j] != 0) {
					int month = i+1;
					int day = j+1;
					System.out.println("trips_pickedup_Madison_Brooklyn_green:" + "  " + "month: " + month + " " + "day: " + day + " " + "trips: " + trips_pickedup_Madison_Brooklyn_green[i][j]);
				}
				
			}
		}
		
		System.out.println("fhv_without_dropOffLocation= " + trips_without_dropOffLocation_fhv);
		System.out.println("yellow_without_dropOffLocation= " + trips_without_dropOffLocation_yellow);
		System.out.println("green_without_dropOffLocation= " + trips_without_dropOffLocation_green);
		
		System.out.println("minutes_per_trip_fhv= " + minutes_per_trip_fhv);
		System.out.println("minutes_per_trip_yellow= " + minutes_per_trip_yellow);
		System.out.println("minutes_per_trip_green= " + minutes_per_trip_green);
		
		System.out.println("avg_trips_per_day = " + avg_trips_per_day);
		
		System.out.println("num_ trips_pickedup_Woodside_Queens: " + num_trips_pickedup_Woodside_Queens);
		
		System.out.println("records_size = " + num_of_records);
		System.out.println("queue_size= " + num_of_trips);

	}
	
	public static void increase_trip_per_day(JsonObject o) {
		int month = 0, day = 0;
		if(o.get("taxiType").toString().equals("\"fhv\"")){
			month = Integer.parseInt(o.get("pickupDateTime").toString().substring(8, 10));
			day = Integer.parseInt(o.get("pickupDateTime").toString().substring(11, 13));		
		}
		else {			
			month = Integer.parseInt(o.get("pickupDateTime").toString().substring(6, 8));
			day = Integer.parseInt(o.get("pickupDateTime").toString().substring(9, 11));
		}
		
		trips_per_day[month - 1][day - 1]++;
		
	}
	
	public static void increase_trip_without_dropOffLocation(JsonObject o) {
		
		if(o.get("dropOffLocationId").toString().equals("\"\\\"\\\"\"") ||
				o.get("dropOffLocationId").toString().equals("\"\"")) {
			

			if(o.get("taxiType").toString().equals("\"fhv\"")){
				trips_without_dropOffLocation_fhv++;
			}
			else if(o.get("taxiType").toString().equals("\"yellow\"")) {
				trips_without_dropOffLocation_yellow++;
			}
			else {
				trips_without_dropOffLocation_green++;
			}
		}
		
	}
	
	public static void increase_trip_pickedup_madison(JsonObject o) {
		/*locationId= 149*/
		//System.out.println(o.get("pickupLocationId").toString());
		if(o.get("pickupLocationId").toString().equals("\"\\\"149\\\"\"") ||
				o.get("pickupLocationId").toString().equals("\"149\"")) {
			
			int month = 0, day = 0;
			if(o.get("taxiType").toString().equals("\"fhv\"")){
				month = Integer.parseInt(o.get("pickupDateTime").toString().substring(8, 10));
				day = Integer.parseInt(o.get("pickupDateTime").toString().substring(11, 13));		
			}
			else {			
				month = Integer.parseInt(o.get("pickupDateTime").toString().substring(6, 8));
				day = Integer.parseInt(o.get("pickupDateTime").toString().substring(9, 11));
			}
			
			if(o.get("taxiType").toString().equals("\"fhv\"")){
				trips_pickedup_Madison_Brooklyn_fhv[month - 1][day - 1]++;
			}
			else if(o.get("taxiType").toString().equals("\"yellow\"")) {
				trips_pickedup_Madison_Brooklyn_yellow[month - 1][day - 1]++;
			}
			else {
				trips_pickedup_Madison_Brooklyn_green[month - 1][day - 1]++;
			}
			
			
		}
	}
	
	public static void minutes_per_trip(JsonObject o) {
		
		int minutes = 0;
		
		int m1 = 0, h1 = 0, m2 = 0, h2 = 0;
		if(o.get("taxiType").toString().equals("\"fhv\"")){
			h1 = Integer.parseInt(o.get("pickupDateTime").toString().substring(14, 16));
			m1 = Integer.parseInt(o.get("pickupDateTime").toString().substring(17, 19));		
			h2 = Integer.parseInt(o.get("dropOffDatetime").toString().substring(14, 16));
			m2 = Integer.parseInt(o.get("dropOffDatetime").toString().substring(17, 19));
		}
		else {			
			h1 = Integer.parseInt(o.get("pickupDateTime").toString().substring(12, 14));
			m1 = Integer.parseInt(o.get("pickupDateTime").toString().substring(15, 17));
			h2 = Integer.parseInt(o.get("dropOffDatetime").toString().substring(12, 14));
			m2 = Integer.parseInt(o.get("dropOffDatetime").toString().substring(15, 17));
		}
		
		minutes = ((h2 - h1) * 60) + (m2 - m1);
		
		
		if(o.get("taxiType").toString().equals("\"fhv\"")){
			num_of_minutes_fhv = num_of_minutes_fhv + minutes;
			num_of_trips_fhv++;
			minutes_per_trip_fhv = num_of_minutes_fhv / num_of_trips_fhv;
		}
		else if(o.get("taxiType").toString().equals("\"yellow\"")) {
			num_of_minutes_yellow = num_of_minutes_yellow + minutes;
			num_of_trips_yellow++;
			minutes_per_trip_yellow = num_of_minutes_yellow / num_of_trips_yellow;
		}
		else {
			num_of_minutes_green = num_of_minutes_green + minutes;
			num_of_trips_green++;
			minutes_per_trip_green = num_of_minutes_green / num_of_trips_green;
		}
	}
	
	public static void avg_trips_per_day() {
		
		for(int i=0;i<trips_per_day.length;i++) {
			for(int j=0;j<trips_per_day[0].length;j++) {
				if(trips_per_day[i][j] != 0) {
					num_of_days++;
				}
			}
		}
		
		avg_trips_per_day = num_of_trips / num_of_days;
		
	}
	
	public static void increase_trip_pickedup_Woodside_Queens(JsonObject o) {
		
		if(o.get("pickupLocationId").toString().equals("\"\\\"260\\\"\"") ||
				o.get("pickupLocationId").toString().equals("\"260\"")) {
			
			num_trips_pickedup_Woodside_Queens++;
		}
	}
	
	public static void write_data_in_file() {
		StringBuilder s = new StringBuilder();
		
		s.append(num_of_records);
		s.append(',');
		s.append(num_of_trips);
		s.append(',');
		s.append(avg_trips_per_day);
		s.append(',');
		s.append(num_trips_pickedup_Woodside_Queens);
		
		String file_name = "results.txt";
		try {
			PrintWriter outputStream = new PrintWriter(file_name);
			outputStream.println(s.toString());
			outputStream.close();
			System.out.println("dddd");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}




