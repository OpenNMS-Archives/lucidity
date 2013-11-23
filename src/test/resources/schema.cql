
CREATE KEYSPACE lucidity_test WITH
       replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE lucidity_test.users (
	id uuid PRIMARY KEY,
	given text,
	surname text,
	email text,
	age int,
	last_updated bigint,
	created timestamp,
	temperature double,
	validated boolean,
	favorites map<text, text>,
	status map<text, text>,
	days set<text>,
	schedule set<text>,
	scores list<int>
);

CREATE TABLE lucidity_test.addresses (id uuid PRIMARY KEY, zipcode text, street text, city text);
CREATE TABLE lucidity_test.users_email_idx (email text PRIMARY KEY, users_id uuid);
CREATE TABLE lucidity_test.users_addresses (users_id uuid, addresses_id uuid, PRIMARY KEY(users_id, addresses_id));
