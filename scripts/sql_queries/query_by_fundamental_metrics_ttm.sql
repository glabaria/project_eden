-- get ADJ FCF and ROCE
WITH
	t0 AS
		(SELECT
			i.symbol,
			i.company_id,
			i.date,
			i.ebitda, i.depreciationandamortization, b.totalassets, b.totalcurrentliabilities,
			operatingincome, interestexpense,
			revenue, costofrevenue,
			c.operatingcashflow, i.netincome,
			freecashflow, stockbasedcompensation,
			i.epsdiluted
--		 	s.numberofshares
		FROM
			income_statement_quarter i
		INNER JOIN
			balance_sheet_quarter b ON i.company_id = b.company_id AND i.date = b.date
		INNER JOIN
			cash_flow_statement_quarter c ON i.company_id = c.company_id AND i.date = c.date
--		INNER JOIN
--		 	shares_fy s ON s.company_id = c.company_id AND s.date = c.date
		WHERE
			b.totalassets - b.totalcurrentliabilities <> 0 AND
			i.calendaryear > 2018),
	ttm AS
		(SELECT symbol, company_id, date,
			SUM(ebitda) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ebitda,
			SUM(depreciationandamortization) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS depreciationandamortization,
			totalassets,
			totalcurrentliabilities,
			--SUM(totalassets) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS totalassets,
			--SUM(totalcurrentliabilities) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS totalcurrentliabilities,
			SUM(operatingincome) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS operatingincome,
			SUM(interestexpense) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS interestexpense,
			SUM(revenue) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue,
			SUM(costofrevenue) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS costofrevenue,
			SUM(operatingcashflow) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS operatingcashflow,
			SUM(netincome) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS netincome,
			SUM(freecashflow) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS freecashflow,
			SUM(stockbasedcompensation) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS stockbasedcompensation,
			SUM(epsdiluted) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS epsdiluted
--			SUM(numberofshares) OVER (PARTITION BY symbol, company_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS numberofshares
			FROM t0),
	t AS
		(SELECT
			symbol,
			company_id,
			date,
			CAST((ebitda - depreciationandamortization) AS real) / (totalassets - totalcurrentliabilities) * 100 AS roce,
			CAST(operatingincome AS real) / (CASE WHEN interestexpense <= 0 THEN NULL ELSE interestexpense END) AS interest_coverate_ratio,
			CAST((revenue - costofrevenue) AS real) / NULLIF(revenue, 0) * 100 AS gross_margin,
			CAST((ebitda - depreciationandamortization) AS real) / NULLIF(revenue, 0) * 100 AS operating_margin,
			CAST((operatingcashflow) AS real) / NULLIF(netincome, 0) * 100 AS cashflow_conversion,
			freecashflow - stockbasedcompensation AS adj_fcf,
			epsdiluted
--		 	numberofshares
		FROM
			ttm)

SELECT xx.symbol, c.companyname, xx.avg_roce, xx.avg_cashflow_conversion, xx.avg_gross_margin, xx.avg_operating_margin, c.sector, c.country, c.exchangeshortname, c.description
FROM
	(SELECT
		symbol,
		company_id,
		AVG(roce) AS avg_roce,
		AVG(cashflow_conversion) AS avg_cashflow_conversion,
		AVG(gross_margin) AS avg_gross_margin,
		AVG(operating_margin) AS avg_operating_margin
	FROM t
	GROUP BY symbol, company_id
	HAVING AVG(cashflow_conversion) >= 100
	ORDER BY avg_roce DESC) xx
INNER JOIN
	company c ON c.id = xx.company_id
WHERE
	avg_roce IS NOT NULL AND c.sector <> 'Financial Services' AND c.sector <> 'Real Estate'
	AND EXTRACT(YEAR FROM c.ipodate) <= 2015 AND exchangeshortname <> 'PNK' AND avg_operating_margin > 0 AND
	isetf IS FALSE and isfund IS FALSE AND avg_roce <= 100 AND avg_roce > 0 AND avg_gross_margin > 0
ORDER BY avg_roce + avg_gross_margin + avg_operating_margin - abs(avg_gross_margin - avg_operating_margin) DESC