/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.associations.collection.types;


import java.util.Map;
import java.util.Set;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
public interface User {

	String getId();

	void setId(String id);

	Map<String, Address> getAddresses();

	void setAddresses(Map<String, Address> addresses);

	Set<String> getNicknames();

	void setNicknames(Set<String> nicknames);
}
