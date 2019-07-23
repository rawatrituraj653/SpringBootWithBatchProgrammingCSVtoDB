package com.st.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.st.beans.Travel;
import com.st.listener.JobExListener;

@Configuration
@EnableBatchProcessing
public class  BatchConfig{
	
	private static final String  INSERT_QUERY="INSERT INTO TRAVELAGENCY(FLIGHTID,FLIGHTNAME,PILOTNAME,AGENTID,TICKETCOST,DISCOUNT,GST,FINALAMOUNT) "
			+ "			VALUES(:flightId,:flightName,:pilotName,:agentId,:ticketCost,:discount,:gst,:finalAmount)"; 
	
	@Autowired
	private JobBuilderFactory jf;
	
	@Autowired
	private StepBuilderFactory sf;
	
	
	@Bean
	public Job job() {
		
		
		return jf.get("job")
				.incrementer(new RunIdIncrementer())
				.listener(listener())
				.start(step())
				.build()				
				;
	}
	
	
	
	
	
	@Bean
	public Step step() {
		
		return sf.get("Step")
			.<Travel,Travel>chunk(4)
			.reader(reader())
			.processor(process())
			.writer(write())
			.build();
	}
	
	
	@Bean
	public ItemReader<Travel> reader(){
		
		FlatFileItemReader<Travel> itemReader=new FlatFileItemReader<>();
		itemReader.setResource(new ClassPathResource("MyData.csv"));
		itemReader.setLineMapper(new DefaultLineMapper<Travel>() {{
					setLineTokenizer(new DelimitedLineTokenizer() {{
						setNames("flightId","flightName","pilotName","agentId","ticketCost");
					}});
					setFieldSetMapper(new BeanWrapperFieldSetMapper<Travel>() {{
						setTargetType(Travel.class);
					}});
		}});
		
		return itemReader;
	}
		
	
	@Bean
	public ItemProcessor<Travel,Travel> process() {
		
		return (item)->{
			double cost=item.getTicketCost();
			item.setDiscount(cost*14/100);
			item.setGst(cost*18/100);
			item.setFinalAmount(cost+item.getGst()-item.getDiscount());
			return item;
		};
	}
	
	
	@Bean
	public ItemWriter<Travel> write(){
			JdbcBatchItemWriter<Travel> itemWriter=new JdbcBatchItemWriter<>();
				itemWriter.setDataSource(ds());
				itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Travel>());
				itemWriter.setSql(INSERT_QUERY);
		return itemWriter;
	}
	
	@Bean
	public DataSource ds() {
		DriverManagerDataSource drds=new DriverManagerDataSource();
		drds.setDriverClassName("com.mysql.jdbc.Driver");
		drds.setUrl("jdbc:mysql://localhost:3306/batch");
		drds.setUsername("root");
		drds.setPassword("root");
		return drds;
	}
	

	@Bean
	public JobExecutionListener listener() {
		return new JobExListener();
	}
	
}