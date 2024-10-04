package com.springbatch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.BeforeJob;

public class MyJobExecutionListener {

    @BeforeJob
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Job name" + jobExecution.getJobInstance().getJobName());
        System.out.println("Job parameters" + jobExecution.getJobParameters());
        System.out.println("Job Start Time" + jobExecution.getStartTime());
    }

    @AfterJob
    public void afterJob(JobExecution jobExecution) {
        System.out.println("Job name" + jobExecution.getJobInstance().getJobName());
        System.out.println("Job parameters" + jobExecution.getJobParameters());
        System.out.println("Job End Time" + jobExecution.getEndTime());
    }
}
