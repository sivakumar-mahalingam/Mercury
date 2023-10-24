--collect
CREATE FUNCTION array_intersection AS 'mercury.udf.collect.ArrayIntersection';
CREATE FUNCTION array_sort AS 'mercury.udf.collect.ArraySort';
CREATE FUNCTION array_subtract AS 'mercury.udf.collect.ArraySubtract';
CREATE FUNCTION array_union AS 'mercury.udf.collect.ArrayUnion';
--{A}=={B}

--statistics
CREATE FUNCTION jaccard_similarity AS 'mercury.udf.statistics.JaccardSimilarity';

