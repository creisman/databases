select a2.fname, a2.lname 
from Actor a1, Actor a2, Casts c1, Casts c2--, Casts c3, Casts c4 
where a1.fname='Sean' and a1.lname='Connery' 
and a1.id = c1.pid 
and c1.mid = c2.mid 
--and c2.pid = c3.pid 
--and c3.mid = c4.mid 
and c2.pid = a2.id;
