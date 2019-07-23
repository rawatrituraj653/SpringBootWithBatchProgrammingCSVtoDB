package com.st.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobExListener implements JobExecutionListener{

	@Override
	public void beforeJob(JobExecution jobExecution) {
		System.out.println("Before Job: "+jobExecution.getStatus().toString());
		
	}
	@Override
	public void afterJob(JobExecution jobExecution) {
		System.out.println("After Job: "+jobExecution.getStatus().toString());
	}
	
}
