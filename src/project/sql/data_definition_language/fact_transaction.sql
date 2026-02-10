CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.FACTTransaction` (
    TransactionID INT64 NOT NULL,
    AccountID INT64 NOT NULL,
    TransactionDate DATETIME,
    Amount INT64,
    TransactionType STRING,
    BranchID INT
);
