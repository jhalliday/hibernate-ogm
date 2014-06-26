/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.associations.collection.types;

import org.hibernate.Session;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Test;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
public class MapUsingUserWithNotNullCollectionTest extends OgmTestCase {

	@Test
	public void testMapAndElementCollection() throws Exception {
		Session session = openSession();

		MapTestLogic mapTestLogic = new MapTestLogic();
		mapTestLogic.testMapAndElementCollection( sessions, session, (Class<? extends User>) getAnnotatedClasses()[0] );

		checkCleanCache();
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { UserWithNotNullCollection.class, Address.class };
	}
}
