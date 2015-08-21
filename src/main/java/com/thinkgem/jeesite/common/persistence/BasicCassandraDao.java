package com.thinkgem.jeesite.common.persistence;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
/**
 * Contain some member variable needed by Cassandra DB and variable them
 */
public abstract class BasicCassandraDao {
	
	protected  Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static Cluster cluster = null;
	protected static Session session = null;
	
	protected String keyspace;
	protected String table;
	
	protected PreparedStatement delete;
	protected PreparedStatement findById;
	protected PreparedStatement insert;
	protected Metadata metadata;

	private String[] addressArray;
	
	@Value("${cassandra.replication.factor}")
	protected String replication_factor;
	
	public BasicCassandraDao(){}
	
	/*
	 * Partition multiple addresses by comma
	 * such as addresses is "192.168.0.1,192.168.0.2" 
	 */
	protected BasicCassandraDao(String addresses, int port, String keySpace, String table)
	{
		this.addressArray = addresses.split(",");
		this.keyspace = keySpace;
		this.table = table;
		
		if(null == cluster)
		{
			cluster = Cluster.builder()
			         .addContactPoints(addressArray).withPort(port)
			         .build();
		}
		
		
	}
	
    /**
     * if nothing need to be pre-created, please give a empty implementation.
     * use this for init function call-back, so sub-class have change to create
     * own params at initialization time . can't put the content in init() in the 
     * constructor. because IOC mechanism, the replication_factor param
     * isn't initialized when constructor is invoked by spring framwork . 
     */
    protected abstract void createPreparedStatement();
	
    /**
     * For spring framwork to initialize the common part. 
     * The code of initialization of Session, Keyspace do not to distribute in
     * sub-class. 
     */
    @PostConstruct
	protected void init()
	{
        creatSession();		
		createKeyspace();
		createPreparedStatement();
	}
	
	private void creatSession() {
		/*
		 * session object as connection pool
		 */
		if(null == session)
		{
			session = cluster.connect();
		}
	}
	
	private void createKeyspace()
	{
        metadata = cluster.getMetadata();
		
		if(null == metadata.getKeyspace(keyspace))
		{
			try
			{
				session.execute("CREATE KEYSPACE " + keyspace + " WITH replication " + 
							"= {'class':'SimpleStrategy', 'replication_factor':" + replication_factor +"};");
			}
			catch(Exception e)
			{
				logger.info(e.toString());	
			}
		}
		else
		{
			logger.info(" pbo keyspace is already existed");
		}
	}
	
	protected BasicCassandraDao(String[] addresses, int port, String keySpace, String table) {
		this.keyspace = keySpace;
		this.table = table;
		
		if(null == cluster)
		{
			cluster = Cluster.builder()
			         .addContactPoints(addresses).withPort(port)
			         .build();
		}
		
		creatSession();
	}
	
	protected Cluster getCluster()
	{
		return cluster;
	}

	protected Session getSession()
	{
		return session;
	}
	
	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}
	
	public void close()
	{
		session.shutdown();
		cluster.shutdown();
	}
	
}