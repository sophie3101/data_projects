-- year range in the dataset
SELECT COUNT(DISTINCT year) as year_range
FROM names_by_year

/* 
#	year_range
1	145
 */

--The time range is 144 years
--popular name of all times (regardless of gender)
SELECT name,
SUM(occurences) AS baby_num
FROM names_by_year
GROUP BY name
HAVING COUNT(distinct year) > 140
ORDER BY SUM(occurences) DESC
LIMIT 10

/*
#	name	baby_num
1	James	5262396
2	John	5196210
3	Robert	4866007
4	Michael	4440391
5	William	4205026
6	Mary	4154332
7	David	3682683
8	Joseph	2672746
9	Richard	2585535
10	Charles	2441151
*/

--top girl name of all time
SELECT name,
SUM(occurences) as girl_baby_num,
RANK()OVER(ORDER BY SUM(occurences) DESC) as rank
FROM names_by_year
WHERE sex='F'
GROUP BY name
LIMIT 10
/*
#	name	girl_baby_num	rank
1	Mary	4139160	1
2	Elizabeth	1681878	2
3	Patricia	1573445	3
4	Jennifer	1471191	4
5	Linda	1454832	5
6	Barbara	1436402	6
7	Margaret	1262307	7
8	Susan	1123232	8
9	Dorothy	1111479	9
10	Sarah	1095724	10
*/


--top boy name of all time
SELECT name,
SUM(occurences) as boy_baby_num,
RANK()OVER(ORDER BY SUM(occurences) DESC) as rank
FROM names_by_year
WHERE sex='M'
GROUP BY name
LIMIT 10
/*
#	name	boy_baby_num	rank
1	James	5238570	1
2	John	5174470	2
3	Robert	4845891	3
4	Michael	4418526	4
5	William	4189004	5
6	David	3669730	6
7	Joseph	2662040	7
8	Richard	2576005	8
9	Charles	2428685	9
10	Thomas	2351624	10
*/


--top gender neutral of all time
with perc_cte AS (
  SELECT name,
  100.0*SUM(occurences) FILTER(WHERE sex='F')/SUM(occurences) as girl_name_perc,
  100.0*SUM(occurences) FILTER(WHERE sex='M')/SUM(occurences) as boy_name_perc
  FROM names_by_year
  GROUP BY name
)
SELECT name
FROM perc_cte 
WHERE abs(girl_name_perc-boy_name_perc) <> 0
ORDER BY abs(girl_name_perc-boy_name_perc)
LIMIT 15

/*
#	name
1	Wisdom
2	Emanuelle
3	Seneca
4	Unknown
5	Tahjae
6	Mycah
7	Tristyn
8	Emerson
9	Jayel
10	Shine
11	Hillery
12	Cree
13	Ronne
14	Landry
15	Sarang
*/

--top gender neutral name of 2024
with perc_cte AS (
SELECT name,
100.0*SUM(occurences) FILTER(WHERE sex='F')/SUM(occurences) as girl_name_perc,
100.0*SUM(occurences) FILTER(WHERE sex='M')/SUM(occurences) as boy_name_perc
FROM names_by_year
WHERE year =2024
GROUP BY name
)

SELECT *
FROM perc_cte 
WHERE abs(girl_name_perc-boy_name_perc) <> 0
ORDER BY abs(girl_name_perc-boy_name_perc)
LIMIT 15

--Charlie, Golden, Arden, Justice, Unknown, Akari, Kit, Makiah, Huntley, Mykah, Larkin, Lennix, Declyn, Alexis, Sovereign

--what name has the highest number of years of being the most popular name 
WITH rank_cte AS (
  SELECT 
    year, 
    name, 
    sex,
    RANK()OVER(PARTITION BY year,sex ORDER BY occurences DESC) as rank
  FROM names_by_year
), top_by_year_cte AS (
  SELECT 
    year, 
    MAX(CASE WHEN sex='F' THEN name END) AS most_popular_girl_name,
    MAX(CASE WHEN sex='M' THEN name END) AS most_popular_boy_name
  FROM rank_cte
  WHERE rank=1
  GROUP BY year
  ORDER BY year DESC
),top_year_count_cte AS (
  SELECT 
    name,
    sex,
    COUNT(year) AS num_year,
    RANK()OVER(PARTITION BY sex ORDER BY COUNT(year) DESC) as rank
  FROM rank_cte
  WHERE rank=1
  GROUP BY name, sex
)

SELECT 
  name, sex, num_year 
FROM top_year_count_cte
WHERE rank < 5
/*
#	name	sex	num_year
1	Michael	M	44
2	John	M	44
3	Robert	M	17
4	Jacob	M	14
5	Mary	F	76
6	Jennifer	F	15
7	Emily	F	12
8	Jessica	F	9

*/
-- names with the longest streak (consecutive year) of being the most popular name
WITH rank_cte AS (
  SELECT 
    year, 
    name, 
    sex,
    RANK()OVER(PARTITION BY year,sex ORDER BY occurences DESC) as rank
  FROM names_by_year
), streak_cte AS (
  SELECT 
    year,name,sex ,
    rank_cte.year - ROW_NUMBER() OVER(PARTITION BY sex ORDER BY year ) as diff
  FROM  rank_cte 
  WHERE rank=1 
  ORDER BY year DESC
), streak_length_cte AS (
  SELECT name, 
          MAX(sex) as sex, 
          COUNT(diff) as num_consecutive_years,
          RANK()OVER(ORDER BY COUNT(diff) DESC) as rank
  FROM streak_cte
  GROUP BY name, diff
)
SELECT 
  name, sex, num_consecutive_years
FROM streak_length_cte
WHERE rank <5

/*
#	name	sex	num_consecutive_years
1	Mary	F	76
2	Michael	M	44
3	John	M	44
4	Robert	M	17
*/
--most favorite name of each year
WITH rank_cte AS (
SELECT 
  year, 
  name, 
  sex,
  RANK()OVER(PARTITION BY year,sex ORDER BY occurences DESC) as rank
FROM names_by_year
)

SELECT 
  year, 
  MAX(CASE WHEN sex='F' THEN name END) AS most_popular_girl_name,
  MAX(CASE WHEN sex='M' THEN name END) AS most_popular_boy_name
FROM rank_cte
WHERE rank=1
GROUP BY year
ORDER BY year DESC

/*
#	year	most_popular_girl_name	most_popular_boy_name
1	2024	Olivia	Liam
2	2023	Olivia	Liam
3	2022	Olivia	Liam
4	2021	Olivia	Liam
5	2020	Olivia	Liam
6	2019	Olivia	Liam
7	2018	Emma	Liam
8	2017	Emma	Liam
9	2016	Emma	Noah
10	2015	Emma	Noah
11	2014	Emma	Noah
12	2013	Sophia	Noah
13	2012	Sophia	Jacob
14	2011	Sophia	Jacob
15	2010	Isabella	Jacob
16	2009	Isabella	Jacob
17	2008	Emma	Jacob
18	2007	Emily	Jacob
19	2006	Emily	Jacob
20	2005	Emily	Jacob
21	2004	Emily	Jacob
22	2003	Emily	Jacob
23	2002	Emily	Jacob
24	2001	Emily	Jacob
25	2000	Emily	Jacob
26	1999	Emily	Jacob
27	1998	Emily	Michael
28	1997	Emily	Michael
29	1996	Emily	Michael
30	1995	Jessica	Michael
31	1994	Jessica	Michael
32	1993	Jessica	Michael
33	1992	Ashley	Michael
34	1991	Ashley	Michael
35	1990	Jessica	Michael
36	1989	Jessica	Michael
37	1988	Jessica	Michael
38	1987	Jessica	Michael
39	1986	Jessica	Michael
40	1985	Jessica	Michael
41	1984	Jennifer	Michael
42	1983	Jennifer	Michael
43	1982	Jennifer	Michael
44	1981	Jennifer	Michael
45	1980	Jennifer	Michael
46	1979	Jennifer	Michael
47	1978	Jennifer	Michael
48	1977	Jennifer	Michael
49	1976	Jennifer	Michael
50	1975	Jennifer	Michael
51	1974	Jennifer	Michael
52	1973	Jennifer	Michael
53	1972	Jennifer	Michael
54	1971	Jennifer	Michael
55	1970	Jennifer	Michael
56	1969	Lisa	Michael
57	1968	Lisa	Michael
58	1967	Lisa	Michael
59	1966	Lisa	Michael
60	1965	Lisa	Michael
61	1964	Lisa	Michael
62	1963	Lisa	Michael
63	1962	Lisa	Michael
64	1961	Mary	Michael
65	1960	Mary	David
66	1959	Mary	Michael
67	1958	Mary	Michael
68	1957	Mary	Michael
69	1956	Mary	Michael
70	1955	Mary	Michael
71	1954	Mary	Michael
72	1953	Mary	Robert
73	1952	Linda	James
74	1951	Linda	James
75	1950	Linda	James
76	1949	Linda	James
77	1948	Linda	James
78	1947	Linda	James
79	1946	Mary	James
80	1945	Mary	James
81	1944	Mary	James
82	1943	Mary	James
83	1942	Mary	James
84	1941	Mary	James
85	1940	Mary	James
86	1939	Mary	Robert
87	1938	Mary	Robert
88	1937	Mary	Robert
89	1936	Mary	Robert
90	1935	Mary	Robert
91	1934	Mary	Robert
92	1933	Mary	Robert
93	1932	Mary	Robert
94	1931	Mary	Robert
95	1930	Mary	Robert
96	1929	Mary	Robert
97	1928	Mary	Robert
98	1927	Mary	Robert
99	1926	Mary	Robert
100	1925	Mary	Robert

*/

--which year has the highest birth 
SELECT 
  year, 
  SUM(occurences) as total_births,
  SUM(occurences) FILTER(WHERE sex='F') as total_girl_births,
  SUM(occurences) FILTER(WHERE sex='M') as total_boy_births
FROM names_by_year
GROUP BY year
ORDER BY total_births DESC
LIMIT 10
/* #	
year	total_births	total_girl_births	total_boy_births
1	1957	4202143	2044843	2157300
2	1959	4157992	2023472	2134520
3	1960	4153815	2021900	2131915
4	1961	4142301	2018361	2123940
5	1958	4133904	2011610	2122294
6	1956	4123037	2007715	2115322
7	1962	4035372	1966632	2068740
8	1955	4015080	1955279	2059801
9	2007	3997683	1922448	2075235
10	1954	3980708	1941914	2038794 */

names represented in the data, find the name that has had the largest percentage increase in popularity since 1980. Largest decrease?
Can you identify names that may have had an even larger increase or decrease in popularity?