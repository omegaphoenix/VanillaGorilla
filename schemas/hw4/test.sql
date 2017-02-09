ANALYZE states, stores, cities;
EXPLAIN SELECT * FROM states;
EXPLAIN SELECT state_id FROM states;
EXPLAIN SELECT store_id, property_costs
        FROM stores, cities, states
        WHERE stores.city_id = cities.city_id AND 
              cities.state_id = states.state_id aND
              state_name = 'Oregon' AND
              property_costs > 500000;
