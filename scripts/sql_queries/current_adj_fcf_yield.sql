SELECT
    ((most_recent_ttm.freecashflow - most_recent_ttm.stockbasedcompensation) /
    (most_recent_price.price * most_recent_shares.n_shares)) * 100 AS adj_fcf_yield
FROM
    (SELECT
        date,
        SUM(freecashflow) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS freecashflow,
        SUM(stockbasedcompensation) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS stockbasedcompensation
     FROM cash_flow_statement_quarter
     WHERE symbol = 'ADBE'
     ORDER BY date DESC
     LIMIT 1) most_recent_ttm,
    (SELECT
        close AS price
     FROM price
     WHERE symbol = 'ADBE'
     ORDER BY date DESC
     LIMIT 1) most_recent_price,
    (SELECT
        weightedaverageshsoutdil AS n_shares
     FROM income_statement_quarter
     WHERE symbol = 'ADBE'
     ORDER BY date DESC
     LIMIT 1) most_recent_shares;