--1. How many stops are in the database.
SELECT COUNT(*) FROM stops;

--2. Find the id value for the stop 'Craiglockhart'
SELECT id FROM stops
  WHERE name = 'Craiglockhart';

--3. Give the id and the name for the stops on the '4' 'LRT' service.
SELECT id, name FROM
  stops JOIN route ON stop=id
  WHERE num = '4' AND company = 'LRT';

--4. The query shown gives the number of routes that visit either London Road (149) or Craiglockhart (53). Run the query and notice the two services that link these stops have a count of 2. Add a HAVING clause to restrict the output to these two routes.
SELECT company, num, COUNT(*)
  FROM route WHERE stop=149 OR stop=53
  GROUP BY company, num
  HAVING COUNT(*) =2;

--5. Show the services from Craiglockhart to London Road.
SELECT a.company, a.num, a.stop, b.stop
  FROM route a JOIN route b ON
    (a.company=b.company AND a.num=b.num)
  JOIN stops c ON a.stop=c.id
  JOIN stops d ON b.stop=d.id
  WHERE c.name = 'Craiglockhart' AND d.name = 'London Road';

--6. Change the query so that the services between 'Craiglockhart' and 'London Road' are shown.
SELECT a.company, a.num, stopa.name, stopb.name
  FROM route a JOIN route b ON
    (a.company=b.company AND a.num=b.num)
    JOIN stops stopa ON (a.stop=stopa.id)
    JOIN stops stopb ON (b.stop=stopb.id)
  WHERE (stopa.name='Craiglockhart' AND stopb.name = 'London Road');

--7. Give a list of all the services which connect stops 115 and 137 ('Haymarket' and 'Leith')
SELECT DISTINCT a.company, a.num
  FROM route a JOIN route b ON
    (a.company=b.company AND a.num=b.num)
  WHERE a.stop IN (115, 137)
    AND b.stop IN (115, 137)
    AND a.stop <> b.stop;

--8. Give a list of the services which connect the stops 'Craiglockhart' and 'Tollcross'
SELECT DISTINCT a.company, a.num 
  FROM route a JOIN route b ON
    (a.company=b.company AND a.num=b.num)
  JOIN stops stopa ON stopa.id=a.stop
  JOIN stops stopb ON stopb.id=b.stop
  WHERE (stopa.name = 'Craiglockhart' AND stopb.name = 'Tollcross');

--9. Give a distinct list of the stops which may be reached from 'Craiglockhart' by taking one bus, including 'Craiglockhart' itself, offered by the LRT company. Include the company and bus no. of the relevant services.
SELECT DISTINCT stopb.name, a.company, a.num
  FROM route a JOIN route b ON
    (a.company=b.company AND a.num=b.num)
  JOIN stops stopa ON a.stop=stopa.id
  JOIN stops stopb ON b.stop=stopb.id
  WHERE stopa.name = 'Craiglockhart' AND a.company='LRT';

--10. Find the routes involving two buses that can go from Craiglockhart to Lochend.
SELECT r1.num, r1.company, s1.name, r4.num, r4.company FROM route r1
  JOIN route r2 ON r1.num=r2.num AND r1.company=r2.company
  JOIN stops s1 ON r2.stop=s1.id
  JOIN route r3 ON s1.id=r3.stop
  JOIN route r4 ON r3.num=r4.num AND r3.company=r4.company
  WHERE r1.stop=(SELECT id FROM stops WHERE name='Craiglockhart')
  AND r4.stop=(SELECT id FROM stops WHERE name='Lochend')
  ORDER BY r1.num, s1.name, r4.num





