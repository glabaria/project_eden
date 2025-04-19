WITH
    t0 AS (
        SELECT
            i.symbol,
            i.company_id,
            i.date,
            i.ebitda,
            i.depreciationandamortization,
            b.totalassets,
            b.totalcurrentliabilities,
            operatingincome,
            interestexpense,
            revenue,
            costofrevenue,
            c.operatingcashflow,
            i.netincome,
            freecashflow,
            stockbasedcompensation,
            i.epsdiluted,
            i.weightedaverageshsoutdil
        FROM
            income_statement_quarter i
        INNER JOIN
            balance_sheet_quarter b ON i.company_id = b.company_id AND i.date = b.date
        INNER JOIN
            cash_flow_statement_quarter c ON i.company_id = c.company_id AND i.date = c.date
        WHERE
            b.totalassets - b.totalcurrentliabilities <> 0
            AND i.symbol = 'MSFT'
    ),
    ttm AS (
        SELECT
            symbol,
            company_id,
            date,
            SUM(ebitda) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ebitda,
            SUM(depreciationandamortization) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS depreciationandamortization,
            totalassets,
            totalcurrentliabilities,
            SUM(operatingincome) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS operatingincome,
            SUM(interestexpense) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS interestexpense,
            SUM(revenue) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue,
            SUM(costofrevenue) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS costofrevenue,
            SUM(operatingcashflow) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS operatingcashflow,
            SUM(netincome) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS netincome,
            SUM(freecashflow) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS freecashflow,
            SUM(stockbasedcompensation) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS stockbasedcompensation,
            SUM(epsdiluted) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS epsdiluted,
            weightedaverageshsoutdil
        FROM t0
    ),
    t AS (
        SELECT
            ttm.symbol,
            ttm.company_id,
            ttm.date,
            CAST((ebitda - depreciationandamortization) AS real) / (totalassets - totalcurrentliabilities) * 100 AS roce,
            CAST(operatingincome AS real) / (CASE WHEN interestexpense <= 0 THEN NULL ELSE interestexpense END) AS interest_coverate_ratio,
            CAST((revenue - costofrevenue) AS real) / NULLIF(revenue, 0) * 100 AS gross_margin,
            CAST((ebitda - depreciationandamortization) AS real) / NULLIF(revenue, 0) * 100 AS operating_margin,
            CAST((operatingcashflow) AS real) / NULLIF(netincome, 0) * 100 AS cashflow_conversion,
            freecashflow - stockbasedcompensation AS adj_fcf,
            epsdiluted,
            weightedaverageshsoutdil
        FROM
            ttm
    )
SELECT
    t.symbol,
    t.date,
    t.roce,
    t.interest_coverate_ratio,
    t.gross_margin,
    t.operating_margin,
    t.cashflow_conversion,
    t.adj_fcf,
    t.epsdiluted,
    weightedaverageshsoutdil * price.price AS market_cap,
    adj_fcf / (weightedaverageshsoutdil * price.price) * 100 AS adj_fcf_yield
FROM
    t
LEFT JOIN LATERAL (
    SELECT
        p.close AS price,
        p.date AS price_date
    FROM
        price p
    WHERE
        p.symbol = t.symbol
    ORDER BY
        ABS(p.date - t.date)
    LIMIT 1
) AS price ON true
ORDER BY
    t.symbol, t.date ASC;