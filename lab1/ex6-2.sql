select a2.fname, a2.lname 
from actor a1, actor a2, casts c1, casts c2, casts c3, casts c4 
where a1.fname='Sean' and a1.lname='Connery' 
and a1.id = c1.pid 
and c1.mid = c2.mid 
and c2.pid = c3.pid 
and c3.mid = c4.mid 
and c2.pid = a2.id;
