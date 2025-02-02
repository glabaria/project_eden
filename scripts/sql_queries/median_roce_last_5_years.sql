-- get ADJ FCF and ROCE
WITH t AS (SELECT 
		i.symbol,
	 	i.date,
		CAST((i.ebitda - i.depreciationandamortization) AS real) / (b.totalassets - b.totalcurrentliabilities) * 100 AS roce,
		CAST(operatingincome AS real) / NULLIF(interestexpense, 0) AS interest_coverate_ratio,
		CAST((revenue - costofrevenue) AS real) / NULLIF(revenue, 0) * 100 AS gross_margin,
		CAST((i.ebitda - i.depreciationandamortization) AS real) / NULLIF(revenue, 0) * 100 AS operating_margin,
	 	freecashflow - stockbasedcompensation AS adj_fcf
	FROM
		income_statement_fy i
	INNER JOIN
		balance_sheet_fy b ON i.company_id = b.company_id AND i.date = b.date
	INNER JOIN
	 	cash_flow_statement_fy c ON i.company_id = c.company_id AND i.date = c.date
	WHERE
		b.totalassets - b.totalcurrentliabilities <> 0 AND
		i.calendaryear >= 2018),
		
-- get min/max date of each company
	min_max_date AS (SELECT 
				symbol, 
				MIN(date) as min_date, 
				MAX(date) as max_date 
			FROM t 
			GROUP BY symbol),
			
-- use previous table to get adj FCF corresponding to min/max date
	min_for_cagr AS (
		SELECT 
			t.symbol, 
			t.adj_fcf AS min_adj_fcf,
			t.gross_margin as min_gross_margin,
			t.operating_margin as min_operating_margin
		FROM min_max_date 
		INNER JOIN t ON t.date = min_max_date.min_date AND t.symbol = min_max_date.symbol
	),

	max_for_cagr AS (
		SELECT 
			t.symbol, 
			t.adj_fcf AS max_adj_fcf,
			t.gross_margin as max_gross_margin,
			t.operating_margin as max_operating_margin
		FROM min_max_date 
		INNER JOIN t ON t.date = min_max_date.max_date AND t.symbol = min_max_date.symbol
	),
	
-- from min_for_cagr and max_for_cagr calculate CAGR
	cagr AS (
		SELECT
			symbol,
			(POWER(CAST(max_adj_fcf AS REAL) / min_adj_fcf, 1 / 5.0) - 1) * 100 AS adj_fcf_cagr_last_5_years,
			(POWER(CAST(max_gross_margin AS REAL) / min_gross_margin, 1 / 5.0) - 1) * 100 AS gross_margin_cagr_last_5_years,
			(POWER(CAST(max_operating_margin AS REAL) / min_operating_margin, 1 / 5.0) - 1) * 100 AS operating_margin_cagr_last_5_years
		FROM 
			(SELECT 
			 	min_for_cagr.symbol, min_adj_fcf, max_adj_fcf, max_gross_margin, min_gross_margin, min_operating_margin, max_operating_margin
			 FROM 
				min_for_cagr 
			 INNER JOIN max_for_cagr ON min_for_cagr.symbol = max_for_cagr.symbol
			 WHERE
			 	min_adj_fcf > 0 AND max_adj_fcf > 0 AND max_gross_margin > 0 AND min_gross_margin > 0 AND max_operating_margin > 0 AND min_operating_margin > 0
			) x
	)
	
-- put everything together display symbol | roce | cagr
SELECT
	tt.*, cagr.*
FROM
	(SELECT
		t.symbol,
		PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY roce) AS median_roce_last_5_years,
	    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY interest_coverate_ratio) AS median_interest_coverate_ratio_last_5_years,
	 	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY gross_margin) AS median_gross_margin_last_5_years,
	 	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY operating_margin) AS median_operating_margin_last_5_years
	FROM
		t
	GROUP BY
		t.symbol) AS tt
LEFT JOIN
	cagr ON tt.symbol = cagr.symbol
WHERE
	adj_fcf_cagr_last_5_years > 15 AND median_roce_last_5_years >= 20 AND median_interest_coverate_ratio_last_5_years >= 10
ORDER BY median_roce_last_5_years DESC