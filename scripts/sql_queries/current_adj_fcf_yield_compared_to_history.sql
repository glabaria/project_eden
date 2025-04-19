WITH symbols AS (
  SELECT UNNEST(ARRAY['V', 'TPL', 'JNJ', 'ADBE']) AS symbol
),

-- 1) “Current” price & shares via DISTINCT ON (single index scan per table)
most_recent_price AS (
  SELECT DISTINCT ON (p.symbol)
    p.symbol,
    p.date    AS date,
    p.close   AS today_price
  FROM price p
  JOIN symbols s
    ON p.symbol = s.symbol
  ORDER BY p.symbol, p.date DESC
),
most_recent_quarter AS (
  SELECT DISTINCT ON (i.symbol)
    i.symbol,
    i.date    AS date,
    i.weightedaverageshsoutdil AS today_shares
  FROM income_statement_quarter i
  JOIN symbols s
    ON i.symbol = s.symbol
  ORDER BY i.symbol, i.date DESC
),

current_price AS (
  SELECT symbol, today_price FROM most_recent_price
),
current_shares AS (
  SELECT symbol, today_shares FROM most_recent_quarter
),

-- 2) Pre‑filter & adjust dates in one pass, then join price/income once
adjusted_data AS (
  SELECT
    c.symbol,
    /* adjust quarter‑end to last business day */
    CASE
      WHEN EXTRACT(DOW FROM c.date)=0 THEN c.date - INTERVAL '2 days'
      WHEN EXTRACT(DOW FROM c.date)=6 THEN c.date - INTERVAL '1 day'
      ELSE c.date
    END AS adj_date,
    c.freecashflow,
    c.stockbasedcompensation,
    i.weightedaverageshsoutdil
  FROM cash_flow_statement_quarter c
  JOIN income_statement_quarter i
    ON i.symbol = c.symbol
   AND i.date   = c.date
  JOIN symbols s
    ON c.symbol = s.symbol
  /* only need the last 20 years of data (widest window) */
  WHERE c.date >= NOW() - INTERVAL '20 years'
),

-- 3) Historical rolling yield for each adj_date
fcf_yield_data AS (
  SELECT
    ad.symbol,
    ad.adj_date   AS date,
    (
      SUM(ad.freecashflow)     OVER w
    - SUM(ad.stockbasedcompensation) OVER w
    )
    /
    (
      /* get price on the adjusted date via direct join */
      p.close * ad.weightedaverageshsoutdil
    )
    * 100  AS adj_fcf_yield
  FROM adjusted_data ad
  JOIN price p
    ON p.symbol = ad.symbol
   AND p.date   = ad.adj_date
  WINDOW w AS (
    PARTITION BY ad.symbol
    ORDER BY ad.adj_date
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  )
),

-- 4) “Present” yield: sum last 4 quarters up to each quarter, then pick only the *latest* quarter per symbol
present_window AS (
  SELECT
    cf.symbol,
    cf.date,
    SUM(cf.freecashflow)     OVER w
  - SUM(cf.stockbasedcompensation) OVER w
    AS fcf_less_stockcomp,
    ROW_NUMBER() OVER (
      PARTITION BY cf.symbol
      ORDER BY cf.date DESC
    ) AS rn
  FROM cash_flow_statement_quarter cf
  JOIN symbols s
    ON cf.symbol = s.symbol
  WINDOW w AS (
    PARTITION BY cf.symbol
    ORDER BY cf.date
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  )
),
present_adj_fcf_yield AS (
  SELECT
    pw.symbol,
    (pw.fcf_less_stockcomp / (cp.today_price * cs.today_shares)) * 100
      AS present_adj_fcf_yield
  FROM present_window pw
  JOIN current_price  cp ON pw.symbol = cp.symbol
  JOIN current_shares cs ON pw.symbol = cs.symbol
  WHERE pw.rn = 1
),

-- 5) Compute medians in each date‑bucket exactly as you did
medians AS (
  SELECT
    symbol,
    range_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY adj_fcf_yield)
      AS median_adj_fcf_yield
  FROM (
    SELECT
      symbol,
      adj_fcf_yield,
      CASE
        WHEN date >= NOW() - INTERVAL '3 years'  THEN '3_years'
        WHEN date >= NOW() - INTERVAL '5 years'  THEN '5_years'
        WHEN date >= NOW() - INTERVAL '10 years' THEN '10_years'
        WHEN date >= NOW() - INTERVAL '15 years' THEN '15_years'
        WHEN date >= NOW() - INTERVAL '20 years' THEN '20_years'
      END AS range_group
    FROM fcf_yield_data
  ) t
  WHERE range_group IS NOT NULL
  GROUP BY symbol, range_group
)

-- 6) Final join + numeric sort on the bucket prefix
SELECT
  p.symbol,
  p.present_adj_fcf_yield,
  m.range_group,
  m.median_adj_fcf_yield
FROM medians m
JOIN present_adj_fcf_yield p
  ON m.symbol = p.symbol
ORDER BY
  p.symbol, CAST (SPLIT_PART(m.range_group, '_', 1) AS INTEGER);
