-- get ADJ FCF and ROCE
WITH t AS
		(SELECT 
			i.symbol,
			i.company_id,
			i.date,
			CAST((i.ebitda - i.depreciationandamortization) AS real) / (b.totalassets - b.totalcurrentliabilities) * 100 AS roce,
			CAST(operatingincome AS real) / (CASE WHEN interestexpense <= 0 THEN NULL ELSE interestexpense END) AS interest_coverate_ratio,
			CAST((revenue - costofrevenue) AS real) / NULLIF(revenue, 0) * 100 AS gross_margin,
			CAST((i.ebitda - i.depreciationandamortization) AS real) / NULLIF(revenue, 0) * 100 AS operating_margin,
			CAST((c.operatingcashflow) AS real) / NULLIF(i.netincome, 0) * 100 AS cashflow_conversion,
			freecashflow - stockbasedcompensation AS adj_fcf,
			i.epsdiluted,
		 	s.numberofshares
		FROM
			income_statement_fy i
		INNER JOIN
			balance_sheet_fy b ON i.company_id = b.company_id AND i.date = b.date
		INNER JOIN
			cash_flow_statement_fy c ON i.company_id = c.company_id AND i.date = c.date
		INNER JOIN
		 	shares_fy s ON s.company_id = c.company_id AND s.date = c.date
		WHERE
			b.totalassets - b.totalcurrentliabilities <> 0 AND
			i.calendaryear >= 2018), --AND
			--i.symbol IN ('META','MSFT','UNH','AAPL','PM','V','GOOGL','TPL','PG','JNJ','CVX','ABBV','CP','EXP','KO','SSD','NVDA','MLI','AXP','AMZN','TMO','PYPL','ADBE','O','HON','GM','BA','COST','MPC', 'FTNT')),
		
-- get min/max date of each company
	min_max_date AS (SELECT 
				company_id,
				MIN(date) as min_date, 
				MAX(date) as max_date 
			FROM t 
			GROUP BY company_id),
			
-- use previous table to get adj FCF corresponding to min/max date
	min_for_cagr AS (
		SELECT 
			t.symbol, 
			t.company_id,
			t.adj_fcf AS min_adj_fcf,
			t.gross_margin as min_gross_margin,
			t.operating_margin as min_operating_margin,
			t.cashflow_conversion as min_cashflow_conversion,
			t.epsdiluted as min_epsdiluted,
			t.numberofshares as min_numberofshares
		FROM min_max_date 
		INNER JOIN t ON t.date = min_max_date.min_date AND t.company_id = min_max_date.company_id
	),

	max_for_cagr AS (
		SELECT 
			t.symbol, 
			t.company_id,
			t.adj_fcf AS max_adj_fcf,
			t.gross_margin as max_gross_margin,
			t.operating_margin as max_operating_margin,
			t.cashflow_conversion as max_cashflow_conversion,
			t.epsdiluted as max_epsdiluted,
			t.numberofshares as max_numberofshares
		FROM min_max_date 
		INNER JOIN t ON t.date = min_max_date.max_date AND t.company_id = min_max_date.company_id
	),
	
-- from min_for_cagr and max_for_cagr calculate CAGR
	cagr AS (
		SELECT
			symbol,
			company_id,
			(POWER(CAST(max_adj_fcf AS REAL) / min_adj_fcf, 1 / 5.0) - 1) * 100 AS adj_fcf_cagr_last_5_years,
			(POWER(CAST(max_gross_margin AS REAL) / min_gross_margin, 1 / 5.0) - 1) * 100 AS gross_margin_cagr_last_5_years,
			(POWER(CAST(max_operating_margin AS REAL) / min_operating_margin, 1 / 5.0) - 1) * 100 AS operating_margin_cagr_last_5_years,
			(POWER(CAST(max_cashflow_conversion AS REAL) / min_cashflow_conversion, 1 / 5.0) - 1) * 100 AS cashflow_conversion_cagr_last_5_year,
			(POWER(CAST(max_epsdiluted AS REAL) / min_epsdiluted, 1 / 5.0) - 1) * 100 AS epsdiluted_cagr_last_5_year,
			(POWER(CAST(max_numberofshares AS REAL) / min_numberofshares, 1 / 5.0) - 1) * 100 AS numberofshares_cagr_last_5_year
		FROM 
			(SELECT 
			 	min_for_cagr.symbol, 
			   	 CASE WHEN min_adj_fcf > 0 THEN min_adj_fcf ELSE NULL END AS min_adj_fcf, 
			 	 CASE WHEN max_adj_fcf > 0 THEN max_adj_fcf ELSE NULL END AS max_adj_fcf,
				 CASE WHEN max_gross_margin > 0 THEN max_gross_margin ELSE NULL END AS max_gross_margin,
				 CASE WHEN min_gross_margin > 0 THEN min_gross_margin ELSE NULL END AS min_gross_margin,
				 CASE WHEN min_operating_margin > 0 THEN min_operating_margin ELSE NULL END AS min_operating_margin,
				 CASE WHEN max_operating_margin > 0 THEN max_operating_margin ELSE NULL END AS max_operating_margin,
				 CASE WHEN min_cashflow_conversion > 0 THEN min_cashflow_conversion ELSE NULL END AS min_cashflow_conversion,
				 CASE WHEN max_cashflow_conversion > 0 THEN max_cashflow_conversion ELSE NULL END AS max_cashflow_conversion,
				 CASE WHEN min_epsdiluted > 0 THEN min_epsdiluted ELSE NULL END AS min_epsdiluted,
				 CASE WHEN max_epsdiluted > 0 THEN max_epsdiluted ELSE NULL END AS max_epsdiluted,
			     CASE WHEN min_numberofshares > 0 THEN min_numberofshares ELSE NULL END AS min_numberofshares,
				 CASE WHEN max_numberofshares > 0 THEN max_numberofshares ELSE NULL END AS max_numberofshares,
				 min_for_cagr.company_id
			 FROM 
				min_for_cagr 
			 INNER JOIN max_for_cagr ON min_for_cagr.company_id = max_for_cagr.company_id
-- 			 WHERE
-- 			 	min_adj_fcf > 0 AND max_adj_fcf > 0 AND max_gross_margin > 0 AND min_gross_margin > 0 AND max_operating_margin > 0 AND min_operating_margin > 0
-- 			 	AND min_cashflow_conversion > 0 AND max_cashflow_conversion > 0
			) x
	)
	
-- put everything together display symbol | roce | cagr
SELECT
	cagr.symbol,
	c.companyname,
	c.country,
	c.industry,
	c.ipodate,
	tt.median_roce_last_5_years, 
	tt.median_interest_coverage_ratio_last_5_years, 
	tt.median_gross_margin_last_5_years,
	tt.median_operating_margin_last_5_years,
	tt.median_cashflow_conversion_last_5_years,
	--tt.median_epsdiluted_last_5_years,
	cagr.adj_fcf_cagr_last_5_years,
	cagr.gross_margin_cagr_last_5_years,
	cagr.operating_margin_cagr_last_5_years,
	cagr.cashflow_conversion_cagr_last_5_year,
	cagr.epsdiluted_cagr_last_5_year,
	cagr.numberofshares_cagr_last_5_year
FROM
	(SELECT
	 	t.company_id,
		PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY roce) AS median_roce_last_5_years,
	    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY interest_coverate_ratio) AS median_interest_coverage_ratio_last_5_years,
	 	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY gross_margin) AS median_gross_margin_last_5_years,
	 	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY operating_margin) AS median_operating_margin_last_5_years,
	 	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY cashflow_conversion) AS median_cashflow_conversion_last_5_years
	 	--PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY epsdiluted) AS median_epsdiluted_last_5_years
	FROM
		t
	GROUP BY
		t.company_id) AS tt
RIGHT JOIN
	cagr ON tt.company_id = cagr.company_id
INNER JOIN
	company c ON c.id = tt.company_id
WHERE
	cagr.adj_fcf_cagr_last_5_years > 5 AND 
 	tt.median_roce_last_5_years >= 10 AND 
	(tt.median_interest_coverage_ratio_last_5_years IS NULL OR tt.median_interest_coverage_ratio_last_5_years >= 5) AND 
 	tt.median_cashflow_conversion_last_5_years >= 95
ORDER BY median_roce_last_5_years DESC
