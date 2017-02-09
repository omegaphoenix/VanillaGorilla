ANALYZE states, stores, cities;
EXPLAIN SELECT * FROM states;
EXPLAIN SELECT state_id FROM states;
EXPLAIN SELECT store_id, property_costs
        FROM stores, cities, states
        WHERE stores.city_id = cities.city_id AND
              cities.state_id = states.state_id AND
              state_name = 'Oregon' AND
              property_costs > 500000;
EXPLAIN SELECT store_id, property_costs
        FROM cities, states, stores
        WHERE stores.city_id = cities.city_id AND
              state_name = 'Oregon' AND
              cities.state_id = states.state_id AND
              property_costs > 500000;
EXPLAIN SELECT store_id, property_costs
        FROM  states, stores, cities
        WHERE stores.city_id = cities.city_id AND
              cities.state_id = states.state_id AND
              property_costs > 500000 AND
              state_name = 'Oregon';
