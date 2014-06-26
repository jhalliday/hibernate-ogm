/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.cassandra.impl;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;


import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.cfg.OgmConfiguration;
import org.hibernate.ogm.datastore.cassandra.CassandraDialect;
import org.hibernate.ogm.datastore.cassandra.dialect.impl.CassandraSequenceGenerator;
import org.hibernate.ogm.datastore.cassandra.dialect.impl.CassandraTypeMapper;
import org.hibernate.ogm.datastore.cassandra.impl.configuration.CassandraConfiguration;
import org.hibernate.ogm.datastore.cassandra.query.parsing.impl.CassandraQueryParserService;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.StartStoppable;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.options.spi.OptionsService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.ogm.util.configurationreader.impl.ConfigurationPropertyReader;

import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import org.hibernate.ogm.datastore.cassandra.logging.impl.Log;
import org.hibernate.ogm.datastore.cassandra.logging.impl.LoggerFactory;

import java.util.*;

/**
 * Created by jhalli on 5/8/14.
 */
public class CassandraDatastoreProvider implements DatastoreProvider, StartStoppable, Configurable, ServiceRegistryAwareService {

	private static final Log log = LoggerFactory.getLogger();

	private ServiceRegistryImplementor serviceRegistry;

	private CassandraConfiguration config;

	private CassandraSequenceGenerator sequenceGenerator;

	private Cluster cluster;
	private Session session;

	private final Map<String, Table> metaDataCache = new HashMap<String, Table>(); // TODO: populate me in start

	@Override
	public void configure(Map configurationValues) {
		OptionsService optionsService = serviceRegistry.getService( OptionsService.class );
		ClassLoaderService classLoaderService = serviceRegistry.getService( ClassLoaderService.class );
		ConfigurationPropertyReader propertyReader = new ConfigurationPropertyReader( configurationValues, classLoaderService );

		config = new CassandraConfiguration( propertyReader, optionsService.context().getGlobalOptions() );
	}

	public Session getSession() {
		return session;
	}

	public Map<String, Table> getMetaDataCache() {
		return metaDataCache;
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return CassandraDialect.class;
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return CassandraQueryParserService.class;
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void start(Configuration configuration, SessionFactoryImplementor factory) {
		if (cluster == null) {
			try {
				log.connectingToCassandra( config.getHost(), config.getPort() );

				cluster = new Cluster.Builder()
						.addContactPoint( config.getHost() )
						.withPort( config.getPort() )
						.build();

				session = cluster.connect( config.getDatabaseName() ); // TODO create/drop?
			}
			catch (RuntimeException e) {
				throw log.unableToInitializeCassandra( e );
			}
		}

		// config->tables->foreignKeys

		this.sequenceGenerator = new CassandraSequenceGenerator(this);

		Iterator<Table> tables = configuration.getTableMappings();
		while ( tables.hasNext() ) {
			Table table = tables.next();

			table.getForeignKeyIterator(); // TODO index these?

			this.metaDataCache.put( table.getName(), table );

			List<String> primaryKeys = new ArrayList<String>();
			if ( table.isPhysicalTable() ) {
				if ( table.hasPrimaryKey() ) {
					for(Object pkColumn : table.getPrimaryKey().getColumns()) {
						primaryKeys.add(  ((Column)pkColumn).getName() );
					}
				}
				List<String> columnNames = new ArrayList<String>();
				List<String> columnTypes = new ArrayList<String>();

				Iterator<Column> columnsIt = (Iterator<Column>) table.getColumnIterator();
				while ( columnsIt.hasNext() ) {
					Column column = columnsIt.next();
					columnNames.add( column.getName() );
					columnTypes.add( ((SimpleValue) column.getValue()).getTypeName() );
				}

				// TODO honor hbm2dll setting (although tests don't use it right now anyhow)
				createColumnFamilyIfNeeded( table.getName(), primaryKeys, columnNames, columnTypes );
			}
		}
	}

	@Override
	public void stop() {
		log.disconnectingFromCassandra();
		session.close();
		session = null;
		cluster.close();
		cluster = null;
		sequenceGenerator = null;
	}

	/////////////

	public void createColumnFamilyIfNeeded(String entityName, List<String> primaryKeyName,
										   List<String> columnNames, List<String> columnTypes) {

		assert(primaryKeyName != null);

		StringBuilder query = new StringBuilder(  );

		// TODO exists not enough - must have same schema
		query.append( "CREATE TABLE IF NOT EXISTS " )
				.append( "\"" )
				.append( entityName )
				.append( "\"" )
				.append( " (" );

		for (int i = 0; i < columnNames.size(); i++) {

			String columnType = columnTypes.get( i );
			String cqlType = CassandraTypeMapper.INSTANCE.hibernateToCQL( columnType );

			query.append( "\"" );
			query.append( columnNames.get( i ) );
			query.append( "\"" );

			query.append( " " ).append( cqlType ).append( ", " );
		}
		query.append( "PRIMARY KEY (" );
		String prefix = "";
		for (String key : primaryKeyName) {
			query.append( prefix );
			prefix = ",";

			query.append( "\"" );
			query.append( key );
			query.append( "\"" );
		}
		query.append( "));" );

		try {
			session.execute( query.toString() );
		} catch(DriverException e) {
			System.out.println(e.toString());
			throw e;
		}
	}


	public CassandraSequenceGenerator getSequenceGenerator() {
		return this.sequenceGenerator;
	}
}