package org.hibernate.ogm.datastore.cassandra.utils;

import com.datastax.driver.core.*;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.cfg.OgmConfiguration;
import org.hibernate.ogm.datastore.cassandra.dialect.impl.ResultSetTupleSnapshot;
import org.hibernate.ogm.datastore.cassandra.impl.CassandraDatastoreProvider;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.options.navigation.GlobalContext;
import org.hibernate.ogm.utils.TestableGridDialect;

import org.hibernate.ogm.datastore.cassandra.logging.impl.Log;
import org.hibernate.ogm.datastore.cassandra.logging.impl.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jhalli on 5/21/14.
 */
public class CassandraTestHelper implements TestableGridDialect {

	private static final Log log = LoggerFactory.getLogger();

	private static CassandraDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService(
				DatastoreProvider.class );
		if ( !( CassandraDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with Cassandra, cannot extract underlying cache" );
		}
		return CassandraDatastoreProvider.class.cast( provider );
	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		return 0;
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		return 0;
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		return 0;
	}

	@Override
	public long getNumberOEmbeddedCollections(SessionFactory sessionFactory) {
		return 0;
	}

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		// CasandraDialect.getTuple

		CassandraDatastoreProvider provider = getProvider( sessionFactory );
		String table = key.getTable();

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];

		StringBuilder query = new StringBuilder( "SELECT * " )
				.append( "FROM " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( " WHERE " )
				.append( "\"" )
				.append( idColumnName )
				.append( "\"" )
				.append( "=?" );

		ResultSet resultSet;
		boolean next;
		PreparedStatement preparedStatement = provider.getSession().prepare( query.toString() ); // TODO NPE
		BoundStatement boundStatement = preparedStatement.bind( key.getColumnValues()[0].toString() );
		resultSet = provider.getSession().execute( boundStatement );

		if (resultSet.isExhausted()) {
			return null;
		}

		Row row = resultSet.one();
		Map<String, Object> result = new HashMap<String, Object>(  );
		for(ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
			String k = definition.getName();
			Object v = definition.getType().deserialize( row.getBytesUnsafe( k ) );
			result.put( k, v );
		}
		return result;
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {

	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		return null;
	}

	@Override
	public GlobalContext<?, ?> configureDatastore(OgmConfiguration configuration) {
		return null;
	}
}
