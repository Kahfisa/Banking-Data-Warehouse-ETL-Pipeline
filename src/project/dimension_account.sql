CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.DIMAccount` (
    AccountID INT64 NOT NULL,
    CustomerID INT64,
    AccountType STRING,
    Balance INT64,
    DateOpened DATETIME,
    Status STRING
);