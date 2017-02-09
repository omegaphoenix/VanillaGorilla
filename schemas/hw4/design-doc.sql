ANALYZE cities, stores, states;
EXPLAIN SELECT * FROM cities WHERE population > 5000000;
EXPLAIN SELECT store_id FROM stores, cities
       WHERE stores.city_id = cities.city_id AND cities.population > 1000000;
EXPLAIN SELECT store_id FROM stores JOIN
       (SELECT city_id FROM cities
           WHERE population > 1000000) AS big_cities
           ON stores.city_id = big_cities.city_id;
EXPLAIN SELECT store_id, property_costs
       FROM stores, cities, states
           WHERE stores.city_id = cities.city_id AND
                   cities.state_id = states.state_id AND
                           state_name = 'Oregon' AND property_costs > 500000;
