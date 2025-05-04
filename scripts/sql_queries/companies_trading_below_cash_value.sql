-- get all companies with more cash on their balance sheets than market cap and have no long term debt
SELECT
	c.symbol,
	c.companyname,
	c.sector,
	c.country,
	b.cashandcashequivalents,
	cf.freecashflow,
	c.mktcap,
	c.description
FROM
	(SELECT symbol, MAX(date) AS date FROM balance_sheet_quarter GROUP BY symbol) b_mrd,
	balance_sheet_quarter b,
	cash_flow_statement_quarter cf,
	company c
WHERE
	b_mrd.symbol = b.symbol AND
	b_mrd.date = b.date AND
	b.symbol = c.symbol AND
	cf.symbol = b.symbol AND
	cf.date = b_mrd.date AND
	b.cashandcashequivalents > c.mktcap AND
	c.country = 'US' AND
	c.sector <> 'Financial Services' AND
	c.isfund IS FALSE AND
	c.isetf IS FALSE AND
	c.isactivelytrading IS TRUE AND
	b.longtermdebt = 0
ORDER BY mktcap DESC