select 
	bill_id, 
	titles::json->0->>'title', 
	congress, 
	split_part(sponsor::json->>'name', ',', 1),
	sponsor::json->>'state',
	sponsor::json->>'district'
from bills limit 5;