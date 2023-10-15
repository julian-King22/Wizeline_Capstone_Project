dim_location_query = """
insert into wizeline-engine.prod.dim_location SELECT
  ROW_NUMBER() OVER () AS id,
  location
FROM
  (SELECT DISTINCT location FROM `wizeline-engine.staging.log_reviews`);
"""


dim_devices_query = """
insert into wizeline-engine.prod.dim_devices SELECT
  ROW_NUMBER() OVER () AS id,
  device
FROM
  (SELECT DISTINCT device FROM `wizeline-engine.staging.log_reviews`);
"""

dim_date_query = """
 insert into wizeline-engine.prod.dim_date SELECT
  ROW_NUMBER() OVER () AS id,
  log_date,
  day,
  month,
  year
FROM
  (SELECT DISTINCT log_date, day, month, year FROM `wizeline-engine.staging.log_reviews`);
"""

dim_os_query = """
insert into wizeline-engine.prod.dim_os SELECT
  ROW_NUMBER() OVER () AS id,
  os,
FROM
  (SELECT DISTINCT os FROM `wizeline-engine.staging.log_reviews`);
"""

fact_movie_analytics = """
insert into wizeline-engine.prod.fact_movie_analytics select 
  user_purchase.customer_id as customer_id,
  logs.os as id_dim_os, logs.location as id_dim_location, 
  logs.device as id_dim_device, 
  logs.log_date as id_dim_date,
  round(SUM(user_purchase.quantity * user_purchase.unit_price),2) amount_spent, 
  SUM(reviews.positive_review) review_score, 
  COUNT(reviews.review_id) review_count ,
from `wizeline-engine.staging.log_reviews` as logs
inner join `wizeline-engine.staging.classified_movie_reviews` as reviews
on logs.log_id = reviews.review_id
left join `wizeline-engine.staging.user_purchase` as user_purchase
on reviews.customer_id = user_purchase.customer_id
where user_purchase.customer_id is not null and user_purchase.quantity >0
group by customer_id, id_dim_os, id_dim_device, id_dim_location, id_dim_date

"""