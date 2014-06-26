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

import org.hibernate.SessionFactory;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.metamodel.source.MetadataImplementor;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.id.impl.OgmSequenceGenerator;
import org.hibernate.ogm.service.impl.ConfigurationService;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jhalli on 6/4/14.
 */
public class CassandraIntegrator implements Integrator {

	// TODO de-duplicate with Neo4j backend version. Or not, since both will vanish once OGM-445 is resolved.

	@Override
	public void integrate(Configuration configuration, SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
		addCassandraObserverIfRequired( sessionFactory );
	}

	@Override
	public void integrate(MetadataImplementor metadata, SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
		addCassandraObserverIfRequired( sessionFactory );
	}

	@Override
	public void disintegrate(SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
		// nothing to do
	}

	private void addCassandraObserverIfRequired(SessionFactoryImplementor sessionFactory) {
		if ( currentDialectIsCassandra( sessionFactory ) ) {
			sessionFactory.addObserver( new SchemaCreator() );
		}
	}

	private boolean currentDialectIsCassandra(SessionFactoryImplementor sessionFactoryImplementor) {
		ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();

		return registry.getService( ConfigurationService.class ).isOgmOn()
				&& ( registry.getService( DatastoreProvider.class ) instanceof CassandraDatastoreProvider );
	}

	/**
	 * Adds the required constraints to the schema db
	 */
	private static class SchemaCreator implements SessionFactoryObserver {

		@Override
		public void sessionFactoryCreated(SessionFactory factory) {
			SessionFactoryImplementor sessionFactoryImplementor = (SessionFactoryImplementor) factory;
			ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();
			CassandraDatastoreProvider provider = (CassandraDatastoreProvider) registry.getService( DatastoreProvider.class );
			Set<String> sequences = sequenceGeneratorKeys( sessionFactoryImplementor, provider );
			if(!sequences.isEmpty()) {
				provider.getSequenceGenerator().createUniqueConstraint( sequences );
			}
		}

		private Set<String> sequenceGeneratorKeys(SessionFactoryImplementor sessionFactoryImplementor, CassandraDatastoreProvider provider) {
			Set<String> sequences = new HashSet<String>();
			Map<String, EntityPersister> entityPersisters = sessionFactoryImplementor.getEntityPersisters();
			for ( Map.Entry<String, EntityPersister> entry : entityPersisters.entrySet() ) {
				EntityPersister persister = entry.getValue();
				IdentifierGenerator identifierGenerator = persister.getIdentifierGenerator();
				if ( identifierGenerator instanceof OgmSequenceGenerator) {
					OgmSequenceGenerator sequenceGenerator = (OgmSequenceGenerator) identifierGenerator;
					sequences.add( sequenceGenerator.generatorKey().toString() );
				}
			}
			return sequences;
		}

		@Override
		public void sessionFactoryClosed(SessionFactory factory) {
			// nothing to do
		}
	}
}
