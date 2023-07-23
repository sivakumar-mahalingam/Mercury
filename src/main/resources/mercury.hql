--collect
CREATE FUNCTION array_intersection AS 'mercury.udf.collect.ArrayIntersection';
CREATE FUNCTION array_union AS 'mercury.udf.collect.ArrayUnion';
CREATE FUNCTION array_sort AS 'mercury.udf.collect.ArraySort';

--statistics
CREATE FUNCTION jaccard_similarity AS 'mercury.udf.statistics.JaccardSimilarity';

