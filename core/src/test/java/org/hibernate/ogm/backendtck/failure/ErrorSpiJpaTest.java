/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.failure;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.transaction.TransactionManager;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.jpa.HibernateEntityManagerFactory;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.dialect.batch.spi.BatchableGridDialect;
import org.hibernate.ogm.dialect.impl.GridDialects;
import org.hibernate.ogm.dialect.optimisticlock.spi.OptimisticLockingAwareGridDialect;
import org.hibernate.ogm.dialect.spi.DuplicateInsertPreventionStrategy;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.failure.ErrorHandler.RollbackContext;
import org.hibernate.ogm.failure.operation.CreateTupleWithKey;
import org.hibernate.ogm.failure.operation.ExecuteBatch;
import org.hibernate.ogm.failure.operation.GridDialectOperation;
import org.hibernate.ogm.failure.operation.InsertOrUpdateTuple;
import org.hibernate.ogm.model.impl.DefaultEntityKeyMetadata;
import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.PackagingRule;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.hibernate.ogm.utils.TestHelper;
import org.hibernate.ogm.utils.jpa.GetterPersistenceUnitInfo;
import org.hibernate.ogm.utils.jpa.JpaTestCase;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for using the error handler SPI with JPA.
 *
 * @author Gunnar Morling
 *
 */
public class ErrorSpiJpaTest  extends JpaTestCase {

	@Rule
	public PackagingRule packaging = new PackagingRule( "persistencexml/transaction-type-jta.xml", Shipment.class );

	@Test
	@SkipByGridDialect(value = GridDialectType.MONGODB, comment = "MongoDB tests runs w/o transaction manager")
	public void onRollbackTriggeredThroughJtaPresentsAppliedInsertOperations() throws Exception {
		Map<String, Object> properties = new HashMap<>();
		properties.putAll( TestHelper.getEnvironmentProperties() );
		properties.put( OgmProperties.ERROR_HANDLER, InvocationTrackingHandler.INSTANCE );
		properties.put( "hibernate.search.default.directory_provider", "ram" );

		EntityManagerFactory emf = Persistence.createEntityManagerFactory( "transaction-type-jta", properties );

		TransactionManager transactionManager = getTransactionManager( emf );

		transactionManager.begin();
		EntityManager em = emf.createEntityManager();

		// given two inserted records
		em.persist( new Shipment( "shipment-1", "INITIAL" ) );
		em.persist( new Shipment( "shipment-2", "INITIAL" ) );

		em.flush();
		em.clear();

		try {
			// when provoking a duplicate-key exception
			em.persist( new Shipment( "shipment-1", "INITIAL" ) );
			transactionManager.commit();
			fail( "Expected exception was not raised" );
		}
		catch (Exception e) {
			// Nothing to do
		}

		// then expect the ops for inserting the two records
		Iterator<RollbackContext> onRollbackInvocations = InvocationTrackingHandler.INSTANCE.getOnRollbackInvocations().iterator();
		Iterator<GridDialectOperation> appliedOperations = onRollbackInvocations.next().getAppliedGridDialectOperations().iterator();
		assertThat( onRollbackInvocations.hasNext() ).isFalse();

		if ( currentDialectHasFacet( BatchableGridDialect.class ) ) {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			GridDialectOperation operation = appliedOperations.next();
			assertThat( operation ).isInstanceOf( ExecuteBatch.class );

			ExecuteBatch batch = operation.as( ExecuteBatch.class );
			Iterator<GridDialectOperation> batchedOperations = batch.getOperations().iterator();
			assertThat( batchedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
			assertThat( batchedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
			assertThat( batchedOperations.hasNext() ).isFalse();
		}
		else {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
		}

		// If LOOK_UP is used for duplicate prevention, the duplicated id will be detected prior to the actual insert
		// itself; otherwise, the CreateTuple call will succeed, and only the insert call will fail
		if ( currentDialectUsesLookupDuplicatePreventionStrategy() ) {
			assertThat( appliedOperations.hasNext() ).isFalse();
		}
		else {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
		}

		transactionManager.begin();
		em.joinTransaction();

		Shipment shipment = em.find( Shipment.class, "shipment-1" );
		if ( shipment != null ) {
			em.remove( shipment );
		}

		shipment = em.find( Shipment.class, "shipment-2" );
		if ( shipment != null ) {
			em.remove( shipment );
		}

		transactionManager.commit();

		em.close();
	}

	@Test
	public void onRollbackTriggeredThroughManualRollbackPresentsAppliedInsertOperations() throws Exception {
		EntityManager em = getFactory().createEntityManager();
		em.getTransaction().begin();

		// given two inserted records
		em.persist( new Shipment( "shipment-1", "INITIAL" ) );
		em.persist( new Shipment( "shipment-2", "INITIAL" ) );

		em.flush();
		em.clear();

		try {
			// when provoking a duplicate-key exception
			em.persist( new Shipment( "shipment-1", "INITIAL" ) );
			em.getTransaction().commit();
			fail( "Expected exception was not raised" );
		}
		catch (Exception e) {
			// Nothing to do
		}

		// then expect the ops for inserting the two records
		Iterator<RollbackContext> onRollbackInvocations = InvocationTrackingHandler.INSTANCE.getOnRollbackInvocations().iterator();
		Iterator<GridDialectOperation> appliedOperations = onRollbackInvocations.next().getAppliedGridDialectOperations().iterator();
		assertThat( onRollbackInvocations.hasNext() ).isFalse();

		if ( currentDialectHasFacet( BatchableGridDialect.class ) ) {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( ExecuteBatch.class );
		}
		else {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
			assertThat( appliedOperations.next() ).isInstanceOf( InsertOrUpdateTuple.class );
		}

		// If LOOK_UP is used for duplicate prevention, the duplicated id will be detected prior to the actual insert
		// itself; otherwise, the CreateTuple call will succeed, and only the insert call will fail
		if ( currentDialectUsesLookupDuplicatePreventionStrategy() ) {
			assertThat( appliedOperations.hasNext() ).isFalse();
		}
		else {
			assertThat( appliedOperations.next() ).isInstanceOf( CreateTupleWithKey.class );
		}

		em.clear();
		em.getTransaction().begin();

		Shipment shipment = em.find( Shipment.class, "shipment-1" );
		if ( shipment != null ) {
			em.remove( shipment );
		}

		shipment = em.find( Shipment.class, "shipment-2" );
		if ( shipment != null ) {
			em.remove( shipment );
		}

		em.getTransaction().commit();
		em.close();
	}

	@After
	public void resetErrorHandler() throws Exception {
		InvocationTrackingHandler.INSTANCE.clear();
	}

	@Override
	public Class<?>[] getEntities() {
		return new Class<?>[] { Shipment.class };
	}

	@Override
	protected void refineInfo(GetterPersistenceUnitInfo info) {
		info.getProperties().put( OgmProperties.ERROR_HANDLER, InvocationTrackingHandler.INSTANCE );
	}

	private boolean currentDialectHasFacet(Class<? extends GridDialect> facet) {
		SessionFactoryImplementor sfi = (SessionFactoryImplementor) ( (HibernateEntityManagerFactory) getFactory() ).getSessionFactory();
		GridDialect gridDialect = sfi.getServiceRegistry().getService( GridDialect.class );
		return GridDialects.hasFacet( gridDialect, OptimisticLockingAwareGridDialect.class );
	}

	private boolean currentDialectUsesLookupDuplicatePreventionStrategy() {
		SessionFactoryImplementor sfi = (SessionFactoryImplementor) ( (HibernateEntityManagerFactory) getFactory() ).getSessionFactory();
		GridDialect gridDialect = sfi.getServiceRegistry().getService( GridDialect.class );
		DefaultEntityKeyMetadata ekm = new DefaultEntityKeyMetadata( "Shipment", new String[]{"id"} );

		return gridDialect.getDuplicateInsertPreventionStrategy( ekm ) == DuplicateInsertPreventionStrategy.LOOK_UP;
	}

	private TransactionManager getTransactionManager(EntityManagerFactory factory) {
		SessionFactoryImplementor sessionFactory = (SessionFactoryImplementor) ( (HibernateEntityManagerFactory) factory )
				.getSessionFactory();
		return sessionFactory.getServiceRegistry().getService( JtaPlatform.class ).retrieveTransactionManager();
	}
}
