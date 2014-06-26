/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.associations.collection.types;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.JoinTable;
import javax.persistence.MapKeyColumn;
import javax.persistence.ElementCollection;
import javax.persistence.JoinColumn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jhalli on 5/21/14.
 */
@Entity
public class UserWithNullableCollection implements User {

	private String id;
	private Map<String, Address> addresses = new HashMap<String, Address>();
	private Set<String> nicknames = new HashSet<String>();

	@Id
	@GeneratedValue(generator = "uuid")
	@GenericGenerator(name = "uuid", strategy = "uuid2")
	@Override
	public String getId() {
		return id;
	}

	@Override
	public void setId(String id) {
		this.id = id;
	}

	@OneToMany
	@JoinTable(name = "User_Address")
	@MapKeyColumn(name = "nick")
	@Override
	public Map<String, Address> getAddresses() {
		return addresses;
	}

	@Override
	public void setAddresses(Map<String, Address> addresses) {
		this.addresses = addresses;
	}

	@ElementCollection
	@JoinTable(name = "Nicks", joinColumns = @JoinColumn(name = "user_id"))
	@Override
	public Set<String> getNicknames() {
		return nicknames;
	}

	@Override
	public void setNicknames(Set<String> nicknames) {
		this.nicknames = nicknames;
	}
}
