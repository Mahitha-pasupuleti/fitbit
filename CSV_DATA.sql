CREATE TABLE registered_users (
  user_id VARCHAR(255) NOT NULL,
  device_id VARCHAR(255) NOT NULL,
  mac_address VARCHAR(255) NOT NULL,
  registration_timestamp VARCHAR(255) NOT NULL
);

CREATE TABLE gym_login (
  mac_address VARCHAR(255),
  gym INT NOT NULL,
  login VARCHAR(255) NOT NULL,
  logout VARCHAR(255) NOT NULL
);
