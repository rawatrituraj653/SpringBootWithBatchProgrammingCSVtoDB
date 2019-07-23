package com.st.process;

import org.springframework.batch.item.ItemProcessor;

import com.st.beans.Travel;

public class MyProcess implements ItemProcessor<Travel,Travel>{

	
	@Override
	public Travel process(Travel item) throws Exception {
		
		item.setDiscount(item.getTicketCost()*3/100);
		item.setGst(item.getGst()*18/100);
		item.setFinalAmount(item.getTicketCost()+item.getGst()-item.getDiscount());
		return item;
	}
}
