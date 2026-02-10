CREATE OR REPLACE PROCEDURE `DWH.DailyTransaction`(start_date DATE, end_date DATE)
BEGIN
  SELECT
    DATE(TransactionDate) AS TransactionDate,
    COUNT(*) AS TotalTransactions,
    SUM(Amount) AS TotalAmount
  FROM `DWH.FACTTransaction`
  WHERE DATE(TransactionDate) BETWEEN start_date AND end_date
  GROUP BY DATE(TransactionDate)
  ORDER BY TransactionDate ASC;
END;

CALL `DWH.DailyTransaction`('2023-01-17','2023-01-22');
