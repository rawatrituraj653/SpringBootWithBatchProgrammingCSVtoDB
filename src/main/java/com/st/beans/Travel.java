package com.st.beans;

import lombok.Data;

@Data
public class Travel {

	private String flightId;
	private String flightName;
	private String pilotName;
	private String agentId;
	private double ticketCost;
	private double discount;
	private double gst;
	private double finalAmount;
	
}

