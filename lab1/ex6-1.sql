select d.fname, d.lname
from actor a, casts c, movie_director m, director d
where a.id=c.pid and c.mid=m.mid and m.did=d.id 
and a.fname='John' and a.lname='Spicer';
