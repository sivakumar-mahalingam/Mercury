--collect
CREATE FUNCTION array_intersection AS 'mercury.udf.collect.ArrayIntersection';
CREATE FUNCTION array_union AS 'mercury.udf.collect.ArrayUnion';

--statistics
CREATE FUNCTION jaccard_similarity AS 'mercury.udf.statistics.JaccardSimilarity';

