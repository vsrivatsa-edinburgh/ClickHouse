DROP TABLE IF EXISTS sf_tokensf_map_keys_test;
DROP TABLE IF EXISTS sf_ngramsf_map_keys_test;

CREATE TABLE sf_tokensf_map_keys_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_tokensf mapKeys(map) TYPE tokensf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_tokensf mapKeys(map_fixed) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO sf_tokensf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text surf filter tokensf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_tokensf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_tokensf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_tokensf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_tokensf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_keys_test WHERE map['K3'] != '';

SELECT 'Map fixed full text surf filter tokensf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokensf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokensf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokensf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokensf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_keys_test WHERE map_fixed['K3'] != '';

DROP TABLE sf_tokensf_map_keys_test;

CREATE TABLE sf_tokensf_map_values_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_values_tokensf mapValues(map) TYPE tokensf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_tokensf mapValues(map_fixed) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO sf_tokensf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text surf filter tokensf mapValues';

SELECT 'Equals with existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_values_test WHERE map['K3'] != '';
SELECT 'Equals with existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE mapContainsValueLike(map, 'V0') SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Equals with non existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE mapContainsValueLike(map, 'V2') SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Not equals with existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE NOT mapContainsValueLike(map, 'V0') SETTINGS force_data_skipping_indices='map_values_tokensf';
SELECT 'Not equals with non existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE NOT mapContainsValueLike(map, 'V2') SETTINGS force_data_skipping_indices='map_values_tokensf';

SELECT 'Map fixed full text surf filter tokensf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_tokensf_map_values_test WHERE map_fixed['K3'] != '';

SELECT 'Equals with existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE mapContainsValueLike(map_fixed, 'V0%') SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Equals with non existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE mapContainsValueLike(map_fixed, 'V2%') SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Not equals with existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE NOT mapContainsValueLike(map_fixed, 'V0%') SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';
SELECT 'Not equals with non existing value';
SELECT * FROM sf_tokensf_map_values_test WHERE NOT mapContainsValueLike(map_fixed, 'V2%') SETTINGS force_data_skipping_indices='map_fixed_values_tokensf';

DROP TABLE sf_tokensf_map_values_test;

CREATE TABLE sf_ngramsf_map_keys_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_ngramsf mapKeys(map) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_ngramsf mapKeys(map_fixed) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO sf_ngramsf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text surf filter ngramsf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_ngramsf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_ngramsf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_ngramsf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_ngramsf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map['K3'] != '';

SELECT 'Map fixed full text surf filter ngramsf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngramsf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngramsf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngramsf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngramsf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_keys_test WHERE map_fixed['K3'] != '';

DROP TABLE sf_ngramsf_map_keys_test;

CREATE TABLE sf_ngramsf_map_values_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_values_ngramsf mapKeys(map) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_ngramsf mapKeys(map_fixed) TYPE ngramsf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO sf_ngramsf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text surf filter ngramsf mapValues';

SELECT 'Equals with existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_ngramsf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_ngramsf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_ngramsf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_ngramsf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_values_test WHERE map['K3'] != '';

SELECT 'Map fixed full text surf filter ngramsf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngramsf';
SELECT 'Equals with non existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngramsf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngramsf';
SELECT 'Not equals with non existing key';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngramsf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM sf_ngramsf_map_values_test WHERE map_fixed['K3'] != '';

DROP TABLE sf_ngramsf_map_values_test;
