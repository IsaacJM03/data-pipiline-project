-- name: q1_top_inventors
SELECT
    i.inventor_id,
    i.name,
    i.country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM relationships r
JOIN inventors i ON i.inventor_id = r.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC, i.name ASC;

-- name: q2_top_companies
SELECT
    c.company_id,
    c.name,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM relationships r
JOIN companies c ON c.company_id = r.company_id
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC, c.name ASC;

-- name: q3_top_countries
SELECT
    i.country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM relationships r
JOIN inventors i ON i.inventor_id = r.inventor_id
GROUP BY i.country
ORDER BY patent_count DESC, i.country ASC;

-- name: q4_patents_per_year
SELECT
    p.year,
    COUNT(*) AS patent_count
FROM patents p
GROUP BY p.year
ORDER BY p.year ASC;

-- name: q5_patent_inventor_company_join
SELECT
    p.patent_id,
    p.title,
    p.year,
    i.inventor_id,
    i.name AS inventor_name,
    i.country,
    c.company_id,
    c.name AS company_name
FROM relationships r
JOIN patents p ON p.patent_id = r.patent_id
JOIN inventors i ON i.inventor_id = r.inventor_id
JOIN companies c ON c.company_id = r.company_id
ORDER BY p.year DESC, p.patent_id ASC;

-- name: q6_country_share_cte
WITH country_patent_counts AS (
    SELECT
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON i.inventor_id = r.inventor_id
    GROUP BY i.country
),
total_patents AS (
    SELECT COUNT(DISTINCT patent_id) AS total FROM relationships
)
SELECT
    cpc.country,
    cpc.patent_count,
    ROUND((1.0 * cpc.patent_count / tp.total) * 100, 2) AS patent_share_pct
FROM country_patent_counts cpc
CROSS JOIN total_patents tp
ORDER BY cpc.patent_count DESC, cpc.country ASC;

-- name: q7_inventor_ranking
SELECT
    inventor_id,
    name,
    country,
    patent_count,
    DENSE_RANK() OVER (ORDER BY patent_count DESC, name ASC) AS inventor_rank
FROM (
    SELECT
        i.inventor_id,
        i.name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
) ranked
ORDER BY inventor_rank ASC, name ASC;
