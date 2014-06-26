/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.associations.collection.types;

import org.hibernate.Session;
import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.OgmTestCase;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.junit.Test;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 * @author Jonathan Halliday <jonathan.halliday@redhat.com>
 */
@SkipByGridDialect(
		value = { GridDialectType.CASSANDRA },
		comment = "Cassandra requires primary key metadata, which ElementCollection doesn't have by default"
)
public class MapTest extends OgmTestCase {

	@Test
	public void testMapAndElementCollection() throws Exception {
		Session session = openSession();

		MapTestLogic mapTestLogic = new MapTestLogic();
		mapTestLogic.testMapAndElementCollection( sessions, session, (Class<? extends User>) getAnnotatedClasses()[0] );

		checkCleanCache();
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { UserWithNullableCollection.class, Address.class };
	}
}
